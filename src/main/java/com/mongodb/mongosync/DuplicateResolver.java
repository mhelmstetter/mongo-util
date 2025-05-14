package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Namespace;
import com.mongodb.shardbalancer.CountingMegachunk;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonValueConverter;
import com.mongodb.util.bson.BsonValueWrapper;

public class DuplicateResolver {

	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);

	private final ShardClient destShardClient;

	// Maps namespace -> (duplicate _id -> List of documents with that _id)
	private Map<String, Map<Object, List<Document>>> duplicateIdToDocsMap = new HashMap<>();

	// Maps namespace -> (chunk -> Set of duplicate _ids in that chunk)
	private Map<String, Map<CountingMegachunk, Set<Object>>> chunkToDuplicateIdsMap = new HashMap<>();

	// Maps shard -> List of chunks that need splitting
	private Map<String, List<ChunkSplitInfo>> shardToSplitInfoMap = new HashMap<>();

	// Map of namespace -> Collection info document -- "key" is shard key
	private Map<String, Document> collectionsMap;

	private final Map<String, RawBsonDocument> destChunksCache = new LinkedHashMap<>();
	private final Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> destChunkMap = new HashMap<>();
	
	private ChunkManager chunkManager;
	private String archiveDbName;

	public DuplicateResolver(ShardClient destShardClient, ChunkManager chunkManager, String archiveDbName) {
		this.destShardClient = destShardClient;
		this.collectionsMap = destShardClient.getCollectionsMap();
		this.archiveDbName = archiveDbName;

		chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
		this.chunkManager = chunkManager;
	}
	
	public void executeSplitsAndMigrations() {
	    logger.debug("executeSplitsAndMigrations: processing {} namespaces with duplicate _id mappings", duplicateIdToDocsMap.keySet().size());
	    
	    // Create a set to track which chunks we've already processed
	    Set<String> processedChunkIdentifiers = new HashSet<>();
	    
	    boolean splitsPerformed = false;
	    
	    // First phase: Handle splits for chunks containing duplicates
	    for (Map.Entry<String, List<ChunkSplitInfo>> entry : shardToSplitInfoMap.entrySet()) {
	        String shardId = entry.getKey();
	        List<ChunkSplitInfo> splitInfoList = entry.getValue();
	        
	        logger.info("Executing splits for shard {}, {} splits planned", shardId, splitInfoList.size());
	        
	        for (ChunkSplitInfo splitInfo : splitInfoList) {
	            CountingMegachunk chunk = splitInfo.getChunk();
	            String chunkId = chunk.getShard() + "_" + chunk.getMin() + "_" + chunk.getMax();

	            // Skip if we've already processed this chunk
	            if (processedChunkIdentifiers.contains(chunkId)) {
	                logger.debug("Skipping already processed chunk: {}", chunkId);
	                continue;
	            }

	            // Get the shard key fields for this namespace
	            Document collMeta = collectionsMap.get(splitInfo.getNamespace());
	            if (collMeta == null) {
	                logger.error("No collection metadata found for namespace: {}", splitInfo.getNamespace());
	                continue;
	            }
	            Document shardKeyDoc = (Document) collMeta.get("key");
	            Set<String> shardKeyFields = shardKeyDoc.keySet();

	            // Extract and sort shard key values from documents with duplicates
	            List<BsonValueWrapper> shardKeyValues = new ArrayList<>();
	            for (Document doc : splitInfo.getSplitDocs()) {
	                shardKeyValues.add(getShardKeyWrapper(shardKeyFields, doc));
	            }
	            Collections.sort(shardKeyValues);

	            // Calculate split points
	            List<Document> splitPoints = calculateSplitPoints(splitInfo.getNamespace(), shardKeyFields,
	                    shardKeyValues);

	            logger.info("Executing {} splits for chunk {} in namespace {}", splitPoints.size(), chunkId,
	                    splitInfo.getNamespace());

	            // Execute each split
	            for (Document splitPoint : splitPoints) {
	                boolean success = destShardClient.splitChunk(splitInfo.getNamespace(), chunk.getMin(),
	                        chunk.getMax(), splitPoint);

	                if (success) {
	                    logger.info("Successfully split chunk at split point: {} for namespace {}", splitPoint, splitInfo.getNamespace());
	                    splitsPerformed = true;
	                } else {
	                    logger.warn("Failed to split chunk at split point: {} for namespace {}", splitPoint,
	                            splitInfo.getNamespace());
	                }
	            }

	            // Mark this chunk as processed
	            processedChunkIdentifiers.add(chunkId);
	        }
	    }
	    
	    // If we performed any splits, refresh the chunk cache before migrations
	    if (splitsPerformed) {
	        for (String namespace : duplicateIdToDocsMap.keySet()) {
	            refreshChunkCache(namespace);
	        }
	        
	        // Allow a moment for the MongoDB metadata to propagate
	        try {
	            Thread.sleep(60000);
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }
	    } else {
	        logger.debug("No splits performed, skipping refresh");
	    }
	    
	    // Reset the processed chunks set before migrations
	    processedChunkIdentifiers.clear();
	    
	    // Process migrations for each namespace with duplicates
	    for (String namespace : duplicateIdToDocsMap.keySet()) {
	        // Get the shard key fields for this namespace
	        Document collMeta = collectionsMap.get(namespace);
	        if (collMeta == null) {
	            logger.error("No collection metadata found for namespace: {}", namespace);
	            continue;
	        }
	        Document shardKeyDoc = (Document)collMeta.get("key");
	        Set<String> shardKeyFields = shardKeyDoc.keySet();
	        
	        // Map of _id -> list of chunks containing documents with that _id
	        Map<Object, List<CountingMegachunk>> idToChunksMap = new HashMap<>();
	        
	        // For each duplicate _id, determine which chunks it's in
	        Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
	        logger.debug("Namespace: {} has {} duplicate _id values to process", namespace, duplicatesMap.size());
	        
	        // Build the mapping of _id -> chunks
	        for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
	            Object id = entry.getKey();
	            List<Document> docsWithSameId = entry.getValue();
	            
	            // Skip if not a duplicate
	            if (docsWithSameId.size() <= 1) continue;
	            
	            // Since there are only 2 duplicates for each _id,
	            // directly find the chunks for each document
	            List<CountingMegachunk> chunksForId = new ArrayList<>(docsWithSameId.size());
	            
	            for (Document doc : docsWithSameId) {
	                BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);
	                
	                NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                if (chunkMap == null) continue;
	                
	                Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyValue);
	                if (chunkEntry == null) continue;
	                
	                CountingMegachunk chunk = chunkEntry.getValue();
	                chunksForId.add(chunk);
	            }
	            
	            // Store the chunks for this _id
	            idToChunksMap.put(id, chunksForId);
	        }
	        
	        // Now process each _id and ensure its duplicates are on different shards
	        int iteration = 0;
	        int maxIterations = 3; // limit iterations to prevent infinite loops
	        boolean movesPerformed;
	        
	        do {
	            iteration++;
	            movesPerformed = false;
	            
	            logger.info("Starting migration iteration {} for namespace {}", iteration, namespace);
	            
	            // Find _ids where both documents are on the same shard
	            List<Map.Entry<Object, List<CountingMegachunk>>> conflictingEntries = new ArrayList<>();
	            
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : idToChunksMap.entrySet()) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                // Skip if there aren't exactly 2 chunks
	                if (chunks.size() != 2) continue;
	                
	                // Check if both chunks are on the same shard
	                if (chunks.get(0).getShard().equals(chunks.get(1).getShard())) {
	                    conflictingEntries.add(entry);
	                }
	            }
	            
	            // If no conflicts, we're done
	            if (conflictingEntries.isEmpty()) {
	                logger.info("No conflicts found in iteration {}", iteration);
	                break;
	            }
	            
	            logger.info("Found {} conflicting _ids in iteration {}", conflictingEntries.size(), iteration);
	            
	            // Process each conflict
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : conflictingEntries) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                String sourceShardId = chunks.get(0).getShard();
	                
	                // Always move the second chunk
	                CountingMegachunk chunkToMove = chunks.get(1);
	                
	                String chunkId = chunkToMove.getShard() + "_" + chunkToMove.getMin() + "_" + chunkToMove.getMax();
	                
	                // Skip if already processed in this iteration
	                if (processedChunkIdentifiers.contains(chunkId)) {
	                    continue;
	                }
	                
	                // Skip if chunk no longer exists
	                if (!chunkExistsWithBoundaries(namespace, chunkToMove.getMin(), chunkToMove.getMax())) {
	                    logger.warn("Chunk with bounds min: {}, max: {} no longer exists, skipping migration", 
	                               chunkToMove.getMin(), chunkToMove.getMax());
	                    processedChunkIdentifiers.add(chunkId);
	                    continue;
	                }
	                
	                // Find a different shard to move to
	                String targetShardId = findDifferentShard(sourceShardId);
	                if (targetShardId == null) {
	                    logger.error("Could not find a different shard for chunk with _id {}", id);
	                    continue;
	                }
	                
	                logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {}", 
	                          chunkToMove.getMin(), chunkToMove.getMax(), sourceShardId, targetShardId);
	                
	                // Move the chunk
	                boolean success = false;
	                int retryCount = 0;
	                int sleep = 5000; // Start with a shorter delay
	                final int MAX_RETRIES = 3;
	                
	                while (!success && retryCount < MAX_RETRIES) {
	                    try {
	                        success = destShardClient.moveChunk(
	                            namespace, chunkToMove.getMin(), chunkToMove.getMax(), 
	                            targetShardId, false, false, true, false, false);
	                        
	                        if (success) {
	                            try {
	                                Thread.sleep(1000);
	                            } catch (InterruptedException e) {
	                                Thread.currentThread().interrupt();
	                            }
	                            logger.info("Successfully moved chunk to shard {}", targetShardId);
	                            
	                            // Mark as processed
	                            processedChunkIdentifiers.add(chunkId);
	                            
	                            // Update our records
	                            chunkToMove.setShard(targetShardId);
	                            
	                            movesPerformed = true;
	                            break;
	                        } else {
	                            try {
	                                Thread.sleep(sleep);
	                            } catch (InterruptedException e) {
	                                Thread.currentThread().interrupt();
	                            }
	                            retryCount++;
	                            sleep = sleep * 2; // Exponential backoff
	                        }
	                    } catch (Exception e) {
	                        logger.warn("Exception moving chunk: {}", e.getMessage());
	                        try {
	                            Thread.sleep(sleep);
	                        } catch (InterruptedException e1) {
	                            Thread.currentThread().interrupt();
	                        }
	                        retryCount++;
	                        sleep = sleep * 2; // Exponential backoff
	                    }
	                }
	            }
	            
	            // If we performed any moves, refresh the chunk cache
	            if (movesPerformed) {
	                refreshChunkCache(namespace);
	                
	                // Clear the processed set for the next iteration
	                processedChunkIdentifiers.clear();
	                
	                // Wait a moment for metadata propagation
	                try {
	                    Thread.sleep(5000);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	            }
	            
	        } while (movesPerformed && iteration < maxIterations);
	        
	        // Final verification
	        int conflictCount = 0;
	        
	        for (Map.Entry<Object, List<CountingMegachunk>> entry : idToChunksMap.entrySet()) {
	            Object id = entry.getKey();
	            List<CountingMegachunk> chunks = entry.getValue();
	            
	            // Skip if there aren't exactly 2 chunks
	            if (chunks.size() != 2) continue;
	            
	            // Check if both chunks are on the same shard
	            if (chunks.get(0).getShard().equals(chunks.get(1).getShard())) {
	                String shardId = chunks.get(0).getShard();
	                
	                // Format chunk information for logging
	                StringBuilder chunkInfo = new StringBuilder();
	                for (CountingMegachunk chunk : chunks) {
	                    if (chunkInfo.length() > 0) {
	                        chunkInfo.append(", ");
	                    }
	                    chunkInfo.append(chunk.getMin()).append("-").append(chunk.getMax());
	                }
	                
	                logger.warn("REMAINING CONFLICT: _id {} has {} chunks on shard {}: {}", 
	                          id, chunks.size(), shardId, chunkInfo.toString());
	                
	                conflictCount++;
	            }
	        }
	        
	        if (conflictCount > 0) {
	            logger.info("Detected cyclic dependencies. Attempting to split conflicting chunks for remaining {} conflicts", conflictCount);
	            
	            // Collect all conflicting _ids and their documents
	            Map<Object, Map<String, List<Document>>> idToShardToDocsMap = new HashMap<>();
	            
	            // First build a mapping of _id -> shard -> documents with that _id on that shard
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : idToChunksMap.entrySet()) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                // Skip if there aren't exactly 2 chunks or if they're on different shards
	                if (chunks.size() != 2 || !chunks.get(0).getShard().equals(chunks.get(1).getShard())) {
	                    continue;
	                }
	                
	                // Get the shard key fields
	                collMeta = collectionsMap.get(namespace);
	                if (collMeta == null) continue;
	                
	                shardKeyDoc = (Document)collMeta.get("key");
	                if (shardKeyDoc == null) continue;
	                
	                shardKeyFields = shardKeyDoc.keySet();
	                if (shardKeyFields.isEmpty()) continue;
	                
	                // Find all documents with this _id in the archive collections
	                Namespace archiveNs1 = new Namespace(archiveDbName, namespace + "_1");
	                Namespace archiveNs2 = new Namespace(archiveDbName, namespace + "_2");
	                
	                List<Document> docs = new ArrayList<>();
	                
	                // Build projection to include _id and shard key fields
	                Document projection = new Document("_id", 1);
	                for (String field : shardKeyFields) {
	                    projection.append(field, 1);
	                }
	                
	                // Find documents in both archive collections
	                destShardClient.getCollection(archiveNs1).find(new Document("_id", id))
	                    .projection(projection).into(docs);
	                destShardClient.getCollection(archiveNs2).find(new Document("_id", id))
	                    .projection(projection).into(docs);
	                
	                // Group documents by shard
	                for (Document doc : docs) {
	                    // Determine which shard this document belongs to
	                    BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);
	                    NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                    if (chunkMap == null) continue;
	                    
	                    Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyValue);
	                    if (chunkEntry == null) continue;
	                    
	                    CountingMegachunk chunk = chunkEntry.getValue();
	                    String shardId = chunk.getShard();
	                    
	                    // Add this document to the map
	                    idToShardToDocsMap
	                        .computeIfAbsent(id, k -> new HashMap<>())
	                        .computeIfAbsent(shardId, k -> new ArrayList<>())
	                        .add(doc);
	                }
	            }
	            
	            // Now for each shard with conflicts, calculate split points
	            Map<String, List<Document>> shardToSplitPointsMap = new HashMap<>();
	            
	            for (Map.Entry<Object, Map<String, List<Document>>> idEntry : idToShardToDocsMap.entrySet()) {
	                for (Map.Entry<String, List<Document>> shardEntry : idEntry.getValue().entrySet()) {
	                    String shardId = shardEntry.getKey();
	                    List<Document> docsOnShard = shardEntry.getValue();
	                    
	                    if (docsOnShard.size() <= 1) continue; // No conflict on this shard
	                    
	                    // Get shard key fields
	                    collMeta = collectionsMap.get(namespace);
	                    if (collMeta == null) continue;
	                    
	                    shardKeyDoc = (Document)collMeta.get("key");
	                    if (shardKeyDoc == null) continue;
	                    
	                    shardKeyFields = shardKeyDoc.keySet();
	                    if (shardKeyFields.isEmpty()) continue;
	                    
	                    String shardKeyField = shardKeyFields.iterator().next();
	                    
	                    // For each document, create a split point in the middle of its shard key value
	                    // and the next document's shard key value
	                    docsOnShard.sort((a, b) -> {
	                        Number valA = (Number) a.get(shardKeyField);
	                        Number valB = (Number) b.get(shardKeyField);
	                        return Double.compare(valA.doubleValue(), valB.doubleValue());
	                    });
	                    
	                    for (int i = 0; i < docsOnShard.size() - 1; i++) {
	                        Number val1 = (Number) docsOnShard.get(i).get(shardKeyField);
	                        Number val2 = (Number) docsOnShard.get(i + 1).get(shardKeyField);
	                        
	                        // Calculate midpoint as split point
	                        double midpoint = (val1.doubleValue() + val2.doubleValue()) / 2;
	                        int splitValue = (int) midpoint;
	                        
	                        // Create a split point document
	                        Document splitPoint = new Document(shardKeyField, splitValue);
	                        
	                        // Add to the map
	                        shardToSplitPointsMap
	                            .computeIfAbsent(shardId, k -> new ArrayList<>())
	                            .add(splitPoint);
	                    }
	                }
	            }
	            
	            // Now execute the splits
	            for (Map.Entry<String, List<Document>> entry : shardToSplitPointsMap.entrySet()) {
	                String shardId = entry.getKey();
	                List<Document> splitPoints = entry.getValue();
	                
	                // Remove duplicates and sort the split points
	                Set<Document> uniqueSplitPoints = new HashSet<>(splitPoints);
	                List<Document> sortedSplitPoints = new ArrayList<>(uniqueSplitPoints);
	                
	                // Sort by the first shard key field
	                String shardKeyField = collectionsMap.get(namespace).get("key", Document.class).keySet().iterator().next();
	                sortedSplitPoints.sort((a, b) -> {
	                    Number valA = (Number) a.get(shardKeyField);
	                    Number valB = (Number) b.get(shardKeyField);
	                    return Double.compare(valA.doubleValue(), valB.doubleValue());
	                });
	                
	                logger.info("Creating {} targeted split points on shard {} to resolve cyclic dependencies", 
	                           sortedSplitPoints.size(), shardId);
	                
	                // Find chunks on this shard
	                List<CountingMegachunk> chunksOnShard = new ArrayList<>();
	                for (NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap : destChunkMap.values()) {
	                    for (CountingMegachunk chunk : chunkMap.values()) {
	                        if (chunk.getShard().equals(shardId)) {
	                            chunksOnShard.add(chunk);
	                        }
	                    }
	                }
	                
	                // For each split point, find which chunk it belongs to and split it
	                for (Document splitPoint : sortedSplitPoints) {
	                    // Get the shard key value from the split point
	                    Number splitValue = (Number) splitPoint.get(shardKeyField);
	                    
	                    // Find which chunk this split point belongs to
	                    CountingMegachunk chunkToSplit = null;
	                    
	                    for (CountingMegachunk chunk : chunksOnShard) {
	                        BsonDocument minDoc = chunk.getMin();
	                        BsonDocument maxDoc = chunk.getMax();
	                        
	                        if (minDoc instanceof BsonDocument && maxDoc instanceof BsonDocument) {
	                            Number minValue = null;
	                            Number maxValue = null;
	                            
	                            if (minDoc.containsKey(shardKeyField)) {
	                                BsonValue minBson = minDoc.get(shardKeyField);
	                                if (minBson instanceof BsonMinKey) {
	                                	minValue = Integer.MIN_VALUE;
	                                } else {
	                                	minValue = (Number)BsonValueConverter.convertBsonValueToObject(minBson);
	                                }
	                            } else {
	                            	logger.warn("minDoc does not contain shardKeyField: {}", minDoc);
	                            }
	                            
	                            if (maxDoc.containsKey(shardKeyField)) {
	                                BsonValue maxBson = maxDoc.get(shardKeyField);
	                                if (maxBson instanceof BsonMaxKey) {
	                                	maxValue = Integer.MAX_VALUE;
	                                } else {
	                                	maxValue = (Number)BsonValueConverter.convertBsonValueToObject(maxBson);
	                                }
	                            } else {
	                            	logger.warn("maxDoc does not contain shardKeyField: {}", maxDoc);
	                            }
	                            
	                            if (minValue != null && maxValue != null) {
	                                if (splitValue.doubleValue() >= minValue.doubleValue() && 
	                                    splitValue.doubleValue() < maxValue.doubleValue()) {
	                                    chunkToSplit = chunk;
	                                    break;
	                                } else {
	                                	logger.warn("splitValue does not fall within max/max range, splitValue: {}, min: {}, max: {}", splitValue, minValue, maxValue);
	                                }
	                            } else {
	                            	logger.warn("minValue or maxValue is null, minValue: {}, maxValue: {}", minValue, maxValue);
	                            }
	                        }
	                    }
	                    
	                    if (chunkToSplit != null) {
	                        logger.info("Splitting chunk with bounds min: {}, max: {} at split point: {}", 
	                                  chunkToSplit.getMin(), chunkToSplit.getMax(), splitPoint);
	                        
	                        boolean success = destShardClient.splitChunk(
	                            namespace, chunkToSplit.getMin(), chunkToSplit.getMax(), splitPoint);
	                        
	                        if (success) {
	                            logger.info("Successfully split chunk at targeted split point: {}", splitPoint);
	                        } else {
	                            logger.warn("Failed to split chunk at targeted split point: {}", splitPoint);
	                        }
	                    } else {
	                        logger.warn("Could not find chunk for split point: {}", splitPoint);
	                    }
	                }
	                
	                // Refresh the chunk cache after splits
	                refreshChunkCache(namespace);
	                
	                // Wait for metadata propagation
	                try {
	                    Thread.sleep(5000);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	            }
	            
	            // After splitting, do one more round of migrations
	            processedChunkIdentifiers.clear();
	            
	            // Perform one more migration iteration
	            logger.info("Starting final migration iteration after targeted splits");
	            
	            // Find remaining _ids where both documents are on the same shard
	            List<Map.Entry<Object, List<CountingMegachunk>>> conflictingEntries = new ArrayList<>();
	            
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : idToChunksMap.entrySet()) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                // Skip if there aren't exactly 2 chunks
	                if (chunks.size() != 2) continue;
	                
	                // Check if both chunks are on the same shard
	                if (chunks.get(0).getShard().equals(chunks.get(1).getShard())) {
	                    conflictingEntries.add(entry);
	                }
	            }
	            
	            logger.info("Found {} conflicting _ids after targeted splits", conflictingEntries.size());
	            
	            // Process each conflict
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : conflictingEntries) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                String sourceShardId = chunks.get(0).getShard();
	                
	                // Always move the second chunk
	                CountingMegachunk chunkToMove = chunks.get(1);
	                
	                String chunkId = chunkToMove.getShard() + "_" + chunkToMove.getMin() + "_" + chunkToMove.getMax();
	                
	                // Skip if already processed
	                if (processedChunkIdentifiers.contains(chunkId)) {
	                    continue;
	                }
	                
	                // Skip if chunk no longer exists
	                if (!chunkExistsWithBoundaries(namespace, chunkToMove.getMin(), chunkToMove.getMax())) {
	                    logger.warn("Chunk with bounds min: {}, max: {} no longer exists, skipping migration", 
	                               chunkToMove.getMin(), chunkToMove.getMax());
	                    processedChunkIdentifiers.add(chunkId);
	                    continue;
	                }
	                
	                // Find a different shard
	                String targetShardId = findDifferentShard(sourceShardId);
	                if (targetShardId == null) {
	                    logger.error("Could not find a different shard for chunk with _id {}", id);
	                    continue;
	                }
	                
	                logger.info("Moving chunk for _id {} from shard {} to shard {}", 
	                           id, sourceShardId, targetShardId);
	                
	                // Move the chunk
	                boolean success = moveChunkWithRetry(namespace, chunkToMove, targetShardId);
	                
	                if (success) {
	                    // Mark as processed
	                    processedChunkIdentifiers.add(chunkId);
	                    
	                    // Update our records
	                    chunkToMove.setShard(targetShardId);
	                }
	            }
	            
	            // Final verification
	            refreshChunkCache(namespace);
	            
	            // Count remaining conflicts after targeted splits
	            conflictCount = 0;
	            
	            for (Map.Entry<Object, List<CountingMegachunk>> entry : idToChunksMap.entrySet()) {
	                Object id = entry.getKey();
	                List<CountingMegachunk> chunks = entry.getValue();
	                
	                // Skip if there aren't exactly 2 chunks
	                if (chunks.size() != 2) continue;
	                
	                // Check if both chunks are on the same shard
	                if (chunks.get(0).getShard().equals(chunks.get(1).getShard())) {
	                    String shardId = chunks.get(0).getShard();
	                    
	                    // Format chunk information for logging
	                    StringBuilder chunkInfo = new StringBuilder();
	                    for (CountingMegachunk chunk : chunks) {
	                        if (chunkInfo.length() > 0) {
	                            chunkInfo.append(", ");
	                        }
	                        chunkInfo.append(chunk.getMin()).append("-").append(chunk.getMax());
	                    }
	                    
	                    logger.warn("REMAINING CONFLICT AFTER TARGETED SPLITS: _id {} has {} chunks on shard {}: {}", 
	                              id, chunks.size(), shardId, chunkInfo.toString());
	                    
	                    conflictCount++;
	                }
	            }
	            
	            if (conflictCount > 0) {
	                logger.warn("Found {} remaining _id conflicts after targeted splits for namespace {}", conflictCount, namespace);
	            } else {
	                logger.info("No remaining _id conflicts after targeted splits for namespace {}", namespace);
	            }
	        }
	        
	        if (conflictCount > 0) {
	            logger.warn("Found {} remaining _id conflicts after migrations for namespace {}", conflictCount, namespace);
	        } else {
	            logger.info("No remaining _id conflicts after migrations for namespace {}", namespace);
	        }
	        
	        logger.debug("Completed processing chunks with duplicates for namespace {}", namespace);
	    }
	    
	    // Refresh the chunk cache after all migrations
	    for (String namespace : duplicateIdToDocsMap.keySet()) {
	        refreshChunkCache(namespace);
	    }
	}
	
	/**
	 * Moves a chunk to a different shard with retry logic to handle transient failures
	 * 
	 * @param namespace The namespace the chunk belongs to
	 * @param chunk The chunk to move
	 * @param targetShardId The target shard to move the chunk to
	 * @return true if the move was successful, false otherwise
	 */
	private boolean moveChunkWithRetry(String namespace, CountingMegachunk chunk, String targetShardId) {
	    boolean success = false;
	    int retryCount = 0;
	    int sleep = 5000; // Start with a shorter delay
	    final int MAX_RETRIES = 3;
	    
	    while (!success && retryCount < MAX_RETRIES) {
	        try {
	            success = destShardClient.moveChunk(
	                namespace, chunk.getMin(), chunk.getMax(), 
	                targetShardId, false, false, true, false, false);
	            
	            if (success) {
	                try {
	                    Thread.sleep(1000);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	                logger.info("Successfully moved chunk to shard {}", targetShardId);
	                return true;
	            } else {
	                try {
	                    Thread.sleep(sleep);
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	                retryCount++;
	                sleep = sleep * 2; // Exponential backoff
	            }
	        } catch (Exception e) {
	            logger.warn("Exception moving chunk: {}", e.getMessage());
	            try {
	                Thread.sleep(sleep);
	            } catch (InterruptedException e1) {
	                Thread.currentThread().interrupt();
	            }
	            retryCount++;
	            sleep = sleep * 2; // Exponential backoff
	        }
	    }
	    
	    return false;
	}
	

	public void determineSplitPoints(String namespace) {
		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
			logger.error("No collection metadata found for namespace: {}", namespace);
			return;
		}
		Document shardKeyDoc = (Document) collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();

		Map<Object, List<Document>> duplicateIdMap = duplicateIdToDocsMap.get(namespace);
		Map<CountingMegachunk, Set<Object>> chunkMap = chunkToDuplicateIdsMap.get(namespace);

		if (duplicateIdMap == null || chunkMap == null) {
			logger.debug("No duplicate mappings found for namespace: {}", namespace);
			return;
		}

		// Group chunks with duplicate _ids by shard
		Map<String, Map<CountingMegachunk, List<Document>>> shardToChunksWithDupes = new HashMap<>();

		NavigableMap<BsonValueWrapper, CountingMegachunk> chunksByShardKey = destChunkMap.get(namespace);
		if (chunksByShardKey == null) {
			logger.error("No chunk map found for namespace: {}", namespace);
			return;
		}

		// Process each duplicate _id
		for (Map.Entry<Object, List<Document>> entry : duplicateIdMap.entrySet()) {
			List<Document> docsWithSameId = entry.getValue();

			// Skip if there's only one document with this _id (not a duplicate)
			if (docsWithSameId.size() <= 1)
				continue;

			// For each document with this _id
			for (Document doc : docsWithSameId) {
				// Get shard key value for this document
				BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);

				Map.Entry<BsonValueWrapper, CountingMegachunk> entry1 = chunksByShardKey.floorEntry(shardKeyValue);
				if (entry1 == null) {
					logger.error("Could not find chunk for document with _id: {}, shardKey: {}", doc.get("_id"),
							shardKeyValue);
					continue;
				}

				CountingMegachunk chunk = entry1.getValue();
				String shard = chunk.getShard();

				// Group by shard then by chunk
				shardToChunksWithDupes.computeIfAbsent(shard, k -> new HashMap<>())
						.computeIfAbsent(chunk, k -> new ArrayList<>()).add(doc);
			}
		}

		// For each shard, determine split points for chunks with duplicates
		for (Map.Entry<String, Map<CountingMegachunk, List<Document>>> shardEntry : shardToChunksWithDupes.entrySet()) {
			String shard = shardEntry.getKey();
			Map<CountingMegachunk, List<Document>> chunksWithDupes = shardEntry.getValue();

			for (Map.Entry<CountingMegachunk, List<Document>> chunkEntry : chunksWithDupes.entrySet()) {
				CountingMegachunk chunk = chunkEntry.getKey();
				List<Document> docsInChunk = chunkEntry.getValue();

				// If there are multiple documents in this chunk with duplicate _ids
				if (docsInChunk.size() > 1) {
					// Extract and sort shard key values
					List<BsonValueWrapper> shardKeyValues = new ArrayList<>();
					for (Document doc : docsInChunk) {
						shardKeyValues.add(getShardKeyWrapper(shardKeyFields, doc));
					}
					Collections.sort(shardKeyValues);

					// Choose appropriate split points
					List<Document> splitPoints = calculateSplitPoints(namespace, shardKeyFields, shardKeyValues);

					// Record chunk split info if we have split points
					if (!splitPoints.isEmpty()) {
						ChunkSplitInfo splitInfo = new ChunkSplitInfo(namespace, chunk, docsInChunk);
						shardToSplitInfoMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(splitInfo);
					}
				}
			}
		}
	}

	private List<Document> calculateSplitPoints(String namespace, Set<String> shardKeyFields,
			List<BsonValueWrapper> sortedShardKeyValues) {
		List<Document> splitPoints = new ArrayList<>();

		// Skip if not enough values to split
		if (sortedShardKeyValues.size() <= 1) {
			return splitPoints;
		}

		// We'll create splits at regular intervals
		int splitInterval = Math.max(1, sortedShardKeyValues.size() / 3); // Don't create too many splits

		for (int i = splitInterval; i < sortedShardKeyValues.size(); i += splitInterval) {
			BsonValueWrapper splitPointValue = sortedShardKeyValues.get(i);

			// Create a document for the split point
			Document splitPoint = new Document();

			// If there's only one field in the shard key, it's simple
			if (shardKeyFields.size() == 1) {
				String keyField = shardKeyFields.iterator().next();

				// Extract the BSON value and convert it properly
				BsonValue bsonValue = splitPointValue.getValue();
				Object keyValue;

				// Handle different BSON types appropriately
				if (bsonValue.isInt32()) {
					keyValue = bsonValue.asInt32().getValue();
				} else if (bsonValue.isInt64()) {
					keyValue = bsonValue.asInt64().getValue();
				} else if (bsonValue.isDouble()) {
					keyValue = bsonValue.asDouble().getValue();
				} else if (bsonValue.isString()) {
					keyValue = bsonValue.asString().getValue();
				} else if (bsonValue.isObjectId()) {
					keyValue = bsonValue.asObjectId().getValue();
				} else if (bsonValue.isBoolean()) {
					keyValue = bsonValue.asBoolean().getValue();
				} else {
					// For other types, use a safe conversion
					keyValue = BsonValueConverter.convertBsonValueToObject(bsonValue);
				}

				splitPoint.append(keyField, keyValue);
			} else {
				// Compound shard keys - create a BsonDocument with all shard key fields
				if (splitPointValue.getValue() instanceof BsonDocument) {
					BsonDocument bsonShardKey = (BsonDocument) splitPointValue.getValue();
					for (String field : shardKeyFields) {
						if (bsonShardKey.containsKey(field)) {
							BsonValue fieldValue = bsonShardKey.get(field);
							Object keyValue = BsonValueConverter.convertBsonValueToObject(fieldValue);
							splitPoint.append(field, keyValue);
						}
					}
				} else {
					logger.warn("Cannot create split point for compound shard key from: {}", splitPointValue);
					continue;
				}
			}

			// Log the split point for debugging
			logger.debug("Created split point: {} for shard key value: {}", splitPoint, splitPointValue);

			splitPoints.add(splitPoint);
		}

		return splitPoints;
	}

	// Helper method to refresh the chunk cache
	private void refreshChunkCache(String namespace) {
	    try {
	        logger.info("Refreshing chunk cache for namespace: {}", namespace);
	        
	        // Clear the existing chunk information for this namespace
	        destChunksCache.clear();
	        if (destChunkMap.containsKey(namespace)) {
	            destChunkMap.get(namespace).clear();
	        }
	        
	        // Reload the chunk information from MongoDB
	        chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
	        
	        logger.info("Successfully refreshed chunk cache for namespace: {}", namespace);
	    } catch (Exception e) {
	        logger.error("Error refreshing chunk cache for namespace {}: {}", namespace, e.getMessage());
	    }
	}

	private boolean chunkExistsWithBoundaries(String namespace, Object min, Object max) {
	    try {
	        NavigableMap<BsonValueWrapper, CountingMegachunk> namespaceChunkMap = destChunkMap.get(namespace);
	        if (namespaceChunkMap == null) {
	            return false;
	        }
	        
	        // Convert min/max to BsonValueWrapper for comparison
	        BsonValueWrapper minWrapper = new BsonValueWrapper(BsonValueConverter.convertToBsonValue(min));
	        BsonValueWrapper maxWrapper = new BsonValueWrapper(BsonValueConverter.convertToBsonValue(max));
	        
	        // Find if chunk exists with matching boundaries
	        for (CountingMegachunk chunk : namespaceChunkMap.values()) {
	            BsonValueWrapper chunkMin = new BsonValueWrapper(BsonValueConverter.convertToBsonValue(chunk.getMin()));
	            BsonValueWrapper chunkMax = new BsonValueWrapper(BsonValueConverter.convertToBsonValue(chunk.getMax()));
	            
	            if (minWrapper.equals(chunkMin) && maxWrapper.equals(chunkMax)) {
	                return true;
	            }
	        }
	        
	        return false;
	    } catch (Exception e) {
	        logger.error("Error checking if chunk exists: {}", e.getMessage());
	        return false;
	    }
	}

	private String findDifferentShard(String currentShardId) {
		// Get available target shards from your existing configuration
		Set<String> targetShards = destShardClient.getShardsMap().keySet();

		// Filter out the current shard
		List<String> otherShards = new ArrayList<>();
		for (String shardId : targetShards) {
			if (!shardId.equals(currentShardId)) {
				otherShards.add(shardId);
			}
		}

		if (otherShards.isEmpty()) {
			return null; // No other shards available
		}

		// Choose a random shard from the available ones
		Random random = new Random();
		return otherShards.get(random.nextInt(otherShards.size()));
	}

	public void buildDuplicateMapping(String namespace, List<Document> duplicates,
			NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap, Set<String> shardKey) {
		// Initialize maps for this namespace if not exists
		duplicateIdToDocsMap.putIfAbsent(namespace, new HashMap<>());
		chunkToDuplicateIdsMap.putIfAbsent(namespace, new HashMap<>());

		logger.debug("Building duplicate mapping for {} with {} duplicate documents", namespace, duplicates.size());
		logger.debug("Chunk map contains {} chunks", chunkMap.size());

		// Log first few chunks for reference
		int chunkCount = 0;
		for (Map.Entry<BsonValueWrapper, CountingMegachunk> entry : chunkMap.entrySet()) {
			if (chunkCount++ < 3) {
				CountingMegachunk chunk = entry.getValue();
				logger.debug("Chunk sample: shard={}, min={}, max={}", chunk.getShard(), chunk.getMin(),
						chunk.getMax());
			} else {
				break;
			}
		}

		int processedCount = 0;
		int errorCount = 0;

		// Process each duplicate document
		for (Document doc : duplicates) {
			// Extract _id and shard key values
			Object id = doc.get("_id");
			BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKey, doc);

			processedCount++;
			if (processedCount <= 5) {
				logger.debug("Processing duplicate document _id: {}, shardKey: {}", id, shardKeyValue);
			}

			// Find which chunk this document belongs to based on shard key
			Map.Entry<BsonValueWrapper, CountingMegachunk> entry = chunkMap.floorEntry(shardKeyValue);
			if (entry == null) {
				errorCount++;
				if (errorCount <= 5) {
					logger.warn("Could not find chunk for document with _id: {}, shardKey: {}", id, shardKeyValue);
				}
				continue;
			}

			CountingMegachunk chunk = entry.getValue();
			BsonValueWrapper min = new BsonValueWrapper(chunk.getMin());
			BsonValueWrapper max = new BsonValueWrapper(chunk.getMax());

			// Add verification check
			if (processedCount <= 5) {
				
				logger.debug("Comparing shard key value: {} (type: {}) with min: {} (type: {}) and max: {} (type: {})",
				        shardKeyValue.getValue(), shardKeyValue.getValue().getClass().getName(),
				        min.getValue(), min.getValue().getClass().getName(),
				        max.getValue(), max.getValue().getClass().getName());
				
				// Verify the chunk really contains this document
				boolean containsDoc = false;
				try {
				    // For RawBsonDocument, we need to extract the nested value
				    BsonValue shardKeyBson = shardKeyValue.getValue();
				    
				    // Extract the actual value from the min/max RawBsonDocuments
				    BsonValue minBson = null;
				    BsonValue maxBson = null;
				    
				    if (min.getValue() instanceof RawBsonDocument) {
				        RawBsonDocument minDoc = (RawBsonDocument)min.getValue();
				        if (minDoc.containsKey("x")) { // Use your actual shard key field name
				            minBson = minDoc.get("x");
				        }
				    }
				    
				    if (max.getValue() instanceof RawBsonDocument) {
				        RawBsonDocument maxDoc = (RawBsonDocument)max.getValue();
				        if (maxDoc.containsKey("x")) { // Use your actual shard key field name
				            maxBson = maxDoc.get("x");
				        }
				    }
				    
				    logger.debug("Extracted values - shardKey: {}, min: {}, max: {}", 
				        shardKeyBson, minBson, maxBson);
				    
				    // Now do the comparison on the extracted values
				    if (minBson != null && maxBson != null) {
				        // If all are numeric, do a numeric comparison
				        if (shardKeyBson.isNumber() && minBson.isNumber() && maxBson.isNumber()) {
				            double shardKeyDouble = shardKeyBson.asNumber().doubleValue();
				            double minDouble = minBson.asNumber().doubleValue();
				            double maxDouble = maxBson.asNumber().doubleValue();
				            
				            containsDoc = shardKeyDouble >= minDouble && shardKeyDouble < maxDouble;
				            
				            logger.debug("Numeric comparison: {} >= {} && {} < {} = {}", 
				                shardKeyDouble, minDouble, shardKeyDouble, maxDouble, containsDoc);
				        } else {
				            // Use BsonValueWrapper for proper comparison
				            BsonValueWrapper shardKeyWrapper = new BsonValueWrapper(shardKeyBson);
				            BsonValueWrapper minWrapper = new BsonValueWrapper(minBson);
				            BsonValueWrapper maxWrapper = new BsonValueWrapper(maxBson);
				            
				            containsDoc = (shardKeyWrapper.compareTo(minWrapper) >= 0 && 
				                          shardKeyWrapper.compareTo(maxWrapper) < 0);
				            
				            logger.debug("BsonValueWrapper comparison: {} >= {} && {} < {} = {}", 
				                shardKeyWrapper, minWrapper, shardKeyWrapper, maxWrapper, containsDoc);
				        }
				    } else {
				        // Fall back to original comparison if we couldn't extract the values
				        containsDoc = shardKeyValue.compareTo(min) >= 0 && shardKeyValue.compareTo(max) < 0;
				        logger.debug("Fallback comparison result: {}", containsDoc);
				    }
				} catch (Exception e) {
				    logger.warn("Error during chunk boundary comparison: {}", e.getMessage(), e);
				    // Default to standard comparison on error
				    containsDoc = shardKeyValue.compareTo(min) >= 0 && shardKeyValue.compareTo(max) < 0;
				}

				if (!containsDoc) {
					logger.warn("Document with _id {} and shardKey {} appears to be outside chunk bounds: [{}, {})", id,
							shardKeyValue, chunk.getMin(), chunk.getMax());
				}
			}

			// Update _id -> documents mapping
			duplicateIdToDocsMap.get(namespace).computeIfAbsent(id, k -> new ArrayList<>()).add(doc);

			// Update chunk -> duplicate _id values mapping
			chunkToDuplicateIdsMap.get(namespace).computeIfAbsent(chunk, k -> new HashSet<>()).add(id);
		}

		logger.debug("Finished building duplicate mapping. Processed {} documents, encountered {} errors.",
				processedCount, errorCount);

		// Log some stats about the mappings
		int idsWithMultipleDocs = 0;
		for (Map.Entry<Object, List<Document>> entry : duplicateIdToDocsMap.get(namespace).entrySet()) {
			if (entry.getValue().size() > 1) {
				idsWithMultipleDocs++;
			}
		}
		logger.debug("Found {} _id values that appear in multiple documents", idsWithMultipleDocs);

		logger.debug("Chunks with duplicate _id values: {}", chunkToDuplicateIdsMap.get(namespace).size());
	}

	/**
	 * Extracts the shard key value from a document and wraps it in a
	 * BsonValueWrapper
	 * 
	 * @param shardKey Set of field names that make up the shard key
	 * @param doc      The document containing the shard key values
	 * @return BsonValueWrapper containing the shard key value(s)
	 */
	private BsonValueWrapper getShardKeyWrapper(Set<String> shardKey, Document doc) {
		// Handle both single field and compound shard keys
		if (shardKey.size() == 1) {
			// Single field shard key
			String keyField = shardKey.iterator().next();
			Object keyValue = doc.get(keyField);

			// Convert the value to a BsonValue
			BsonValue bsonValue = BsonValueConverter.convertToBsonValue(keyValue);
			return new BsonValueWrapper(bsonValue);
		} else {
			// Compound shard key - create a BsonDocument with all shard key fields
			BsonDocument shardKeyDoc = new BsonDocument();

			for (String keyField : shardKey) {
				Object keyValue = doc.get(keyField);
				if (keyValue != null) { // Ensure the key exists in the document
					BsonValue bsonValue = BsonValueConverter.convertToBsonValue(keyValue);
					shardKeyDoc.append(keyField, bsonValue);
				} else {
					logger.warn("Missing shard key field {} in document with _id: {}", keyField, doc.get("_id"));
					// Handle missing shard key field - could use a default value or throw an
					// exception
					shardKeyDoc.append(keyField, new BsonNull());
				}
			}
			return new BsonValueWrapper(shardKeyDoc);
		}
	}

	// Helper class to store information about chunks that need splitting
	private static class ChunkSplitInfo {
		private final String namespace;
		private final CountingMegachunk chunk;
		private final List<Document> splitDocs;

		public ChunkSplitInfo(String namespace, CountingMegachunk chunk, List<Document> splitDocs) {
			this.namespace = namespace;
			this.chunk = chunk;
			this.splitDocs = splitDocs;
		}

		public String getNamespace() {
			return namespace;
		}

		public CountingMegachunk getChunk() {
			return chunk;
		}

		public List<Document> getSplitDocs() {
			return splitDocs;
		}
	}
}