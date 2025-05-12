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
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    

    public DuplicateResolver(ShardClient destShardClient, ChunkManager chunkManager) {
        this.destShardClient = destShardClient;
        this.collectionsMap = destShardClient.getCollectionsMap();
        
       
        chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
    }

    public void executeSplitsAndMigrations() {
        logger.debug("executeSplitsAndMigrations");
        
        // Log duplicate mappings size
        logger.debug("Processing {} namespaces with duplicate _id mappings", duplicateIdToDocsMap.keySet().size());
        
        // Process each namespace
        for (String namespace : duplicateIdToDocsMap.keySet()) {
            // Get the shard key fields for this namespace
            Document collMeta = collectionsMap.get(namespace);
            if (collMeta == null) {
                logger.error("No collection metadata found for namespace: {}", namespace);
                continue;
            }
            Document shardKeyDoc = (Document)collMeta.get("key");
            Set<String> shardKeyFields = shardKeyDoc.keySet();
            
            // First identify duplicate _ids that need to be separated
            Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
            logger.debug("Namespace: {} has {} duplicate _id values to process", namespace, duplicatesMap.size());
            
            int dupeCount = 0;
            // For each duplicate _id value
            for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
                Object duplicateId = entry.getKey();
                List<Document> docsWithSameId = entry.getValue();

                // Skip if there's only one document with this _id (not a duplicate)
                if (docsWithSameId.size() <= 1)
                    continue;
                
                dupeCount++;
                if (dupeCount <= 5) {  // Log first 5 duplicates for diagnostics
                    logger.debug("Duplicate _id value: {} appears in {} documents", duplicateId, docsWithSameId.size());
                }

                // Map to track which shard keys are on which chunks
                Map<CountingMegachunk, List<Document>> chunkToDocsMap = new HashMap<>();
                
                // Determine which chunk each document with this duplicate _id belongs to
                for (Document doc : docsWithSameId) {
                    BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);
                    
                    // Use the chunkMap that was passed as parameter to this method
                    if (destChunkMap == null) {
                        logger.error("No chunk map provided for namespace: {}", namespace);
                        continue;
                    }
                    
                    NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
                    Map.Entry<BsonValueWrapper, CountingMegachunk> entry1 = chunkMap.floorEntry(shardKeyValue);
                    if (entry1 == null) {
                        logger.error("Could not find chunk for document with _id: {}, shardKey: {}", 
                                    doc.get("_id"), shardKeyValue);
                        continue;
                    }
                    
                    CountingMegachunk chunk = entry1.getValue();
                    chunkToDocsMap.computeIfAbsent(chunk, k -> new ArrayList<>()).add(doc);
                }
                
                // Group chunks by shard
                Map<String, List<CountingMegachunk>> chunksByShardMap = new HashMap<>();
                for (CountingMegachunk chunk : chunkToDocsMap.keySet()) {
                    String shardId = chunk.getShard();
                    chunksByShardMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(chunk);
                }
                
                // Log chunk distribution across shards
                if (dupeCount <= 5) {
                    logger.debug("Duplicate _id value: {} distribution across shards:", duplicateId);
                    for (Map.Entry<String, List<CountingMegachunk>> shardEntry : chunksByShardMap.entrySet()) {
                        logger.debug("  - Shard: {} has {} chunks with documents having this _id", 
                                    shardEntry.getKey(), shardEntry.getValue().size());
                    }
                }

                // For each shard that has multiple chunks with this duplicate
                for (Map.Entry<String, List<CountingMegachunk>> shardEntry : chunksByShardMap.entrySet()) {
                    String shardId = shardEntry.getKey();
                    List<CountingMegachunk> chunksInShard = shardEntry.getValue();

                    // If there's more than one chunk with this duplicate in the same shard
                    if (chunksInShard.size() > 1) {
                        // We need to migrate at least one of these chunks to a different shard
                        handleChunksInSameShard(namespace, duplicateId, shardId, chunksInShard, chunkToDocsMap);
                    }
                }
                
                // If duplicates exist across multiple shards, we're good
                // If all duplicates are in one shard but different chunks, 
                // we need to move at least one chunk to another shard
                if (chunksByShardMap.size() == 1 && chunkToDocsMap.size() > 1) {
                    String shardId = chunksByShardMap.keySet().iterator().next();
                    List<CountingMegachunk> chunksInShard = new ArrayList<>(chunkToDocsMap.keySet());
                    handleChunksInSameShard(namespace, duplicateId, shardId, chunksInShard, chunkToDocsMap);
                }
            }
            logger.debug("Total of {} duplicate _id values processed for namespace {}", dupeCount, namespace);
        }
    }
    
    public void determineSplitPoints(String namespace) {
        Document collMeta = collectionsMap.get(namespace);
        if (collMeta == null) {
            logger.error("No collection metadata found for namespace: {}", namespace);
            return;
        }
        Document shardKeyDoc = (Document)collMeta.get("key");
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
                    logger.error("Could not find chunk for document with _id: {}, shardKey: {}", 
                                doc.get("_id"), shardKeyValue);
                    continue;
                }
                
                CountingMegachunk chunk = entry1.getValue();
                String shard = chunk.getShard();
                
                // Group by shard then by chunk
                shardToChunksWithDupes
                    .computeIfAbsent(shard, k -> new HashMap<>())
                    .computeIfAbsent(chunk, k -> new ArrayList<>())
                    .add(doc);
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

    // Helper method to calculate split points based on shard key values
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
                Object keyValue = BsonValueConverter.convertBsonValueToObject(splitPointValue.getValue());
                splitPoint.append(keyField, keyValue);
            } else {
                // For compound shard keys, we need to extract each component
                if (splitPointValue.getValue() instanceof BsonDocument) {
                    BsonDocument bsonShardKey = (BsonDocument) splitPointValue.getValue();
                    for (String field : shardKeyFields) {
                        if (bsonShardKey.containsKey(field)) {
                            BsonValue fieldValue = bsonShardKey.get(field);
                            splitPoint.append(field, BsonValueConverter.convertBsonValueToObject(fieldValue));
                        }
                    }
                } else {
                    logger.warn("Cannot create split point for compound shard key from: {}", splitPointValue);
                    continue;
                }
            }
            
            splitPoints.add(splitPoint);
        }
        
        return splitPoints;
    }

    private void handleChunksInSameShard(String namespace, Object duplicateId, String sourceShardId,
            List<CountingMegachunk> chunksInShard, Map<CountingMegachunk, List<Document>> chunkToDocsMap) {
        logger.info("Handling {} chunks in shard {} with duplicate _id value: {}", 
                    chunksInShard.size(), sourceShardId, duplicateId);

        // For each chunk except the first one, try to move it to a different shard
        for (int i = 1; i < chunksInShard.size(); i++) {
            CountingMegachunk chunk = chunksInShard.get(i);

            // Check if we need to split this chunk first
            if (chunkToDuplicateIdsMap.containsKey(namespace)
                    && chunkToDuplicateIdsMap.get(namespace).containsKey(chunk)
                    && chunkToDuplicateIdsMap.get(namespace).get(chunk).size() > 1) {

                // This chunk contains multiple duplicate _id values, so we should split it first
                List<Document> docsInChunk = chunkToDocsMap.get(chunk);
                splitChunkForMultipleDuplicates(namespace, chunk, docsInChunk);
            }

            // Get a different target shard
            String targetShardId = findDifferentShard(sourceShardId);
            if (targetShardId != null) {
                logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {}", 
                            chunk.getMin(), chunk.getMax(), sourceShardId, targetShardId);

                // Use the existing moveChunk method
                boolean success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), 
                                                            targetShardId, false, false, true, false, false);

                if (success) {
                    logger.info("Successfully moved chunk to shard {}", targetShardId);
                } else {
                    logger.error("Failed to move chunk to shard {}", targetShardId);
                }
            } else {
                logger.error("Could not find a different shard to move chunk to from shard {}", sourceShardId);
            }
        }
    }

    private void splitChunkForMultipleDuplicates(String namespace, CountingMegachunk chunk, List<Document> duplicateDocs) {
        // Get the shard key fields for this namespace
        Document collMeta = collectionsMap.get(namespace);
        if (collMeta == null) {
            logger.error("No collection metadata found for namespace: {}", namespace);
            return;
        }
        Document shardKeyDoc = (Document)collMeta.get("key");
        Set<String> shardKeyFields = shardKeyDoc.keySet();
        
        // Extract shard key values from documents
        List<BsonValueWrapper> shardKeyValues = new ArrayList<>();
        for (Document doc : duplicateDocs) {
            shardKeyValues.add(getShardKeyWrapper(shardKeyFields, doc));
        }
        
        // Sort shard key values
        Collections.sort(shardKeyValues, (a, b) -> a.compareTo(b));

        logger.info("Preparing to split chunk for namespace {} with {} documents having duplicate _ids", 
                   namespace, duplicateDocs.size());
        logger.debug("Chunk details: shard={}, min={}, max={}", 
                    chunk.getShard(), chunk.getMin(), chunk.getMax());
        
        // We'll split at every nth shard key value to keep the number of splits manageable
        int splitInterval = Math.max(1, shardKeyValues.size() / 3); // Don't create too many splits
        logger.debug("Using split interval of {} (will create approximately {} splits)", 
                    splitInterval, shardKeyValues.size() / splitInterval);

        for (int i = splitInterval; i < shardKeyValues.size(); i += splitInterval) {
            BsonValueWrapper splitPointShardKeyValue = shardKeyValues.get(i);

            // Create a document for the split point
            Document splitPoint = new Document();
            
            // If there's only one field in the shard key, it's simple
            if (shardKeyFields.size() == 1) {
                String keyField = shardKeyFields.iterator().next();
                BsonValue bsonValue = BsonValueConverter.convertToBsonValue(splitPointShardKeyValue);
                Object keyValue = BsonValueConverter.convertBsonValueToObject(bsonValue);
                splitPoint.append(keyField, keyValue);
            } else {
                // For compound shard keys, we need to extract each component
                if (splitPointShardKeyValue.getValue() instanceof BsonDocument) {
                    BsonDocument bsonShardKey = (BsonDocument) splitPointShardKeyValue.getValue();
                    for (String field : shardKeyFields) {
                        if (bsonShardKey.containsKey(field)) {
                            BsonValue fieldValue = bsonShardKey.get(field);
                            splitPoint.append(field, BsonValueConverter.convertBsonValueToObject(fieldValue));
                        }
                    }
                } else {
                    logger.warn("Cannot create split point for compound shard key from: {}", splitPointShardKeyValue);
                    continue;
                }
            }

            logger.info("Attempting to split chunk at shard key value: {} for namespace {}", 
                       splitPointShardKeyValue, namespace);

            // Call the split command
            boolean success = destShardClient.splitChunk(namespace, chunk.getMin(), chunk.getMax(), splitPoint);

            if (success) {
                logger.info("Successfully split chunk at shard key value: {}", splitPointShardKeyValue);
            } else {
                logger.warn("Failed to split chunk at shard key value: {}", splitPointShardKeyValue);
            }
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
        
        logger.debug("Building duplicate mapping for {} with {} duplicate documents", 
                    namespace, duplicates.size());
        logger.debug("Chunk map contains {} chunks", chunkMap.size());
        
        // Log first few chunks for reference
        int chunkCount = 0;
        for (Map.Entry<BsonValueWrapper, CountingMegachunk> entry : chunkMap.entrySet()) {
            if (chunkCount++ < 3) {
                CountingMegachunk chunk = entry.getValue();
                logger.debug("Chunk sample: shard={}, min={}, max={}", 
                            chunk.getShard(), chunk.getMin(), chunk.getMax());
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
                // Verify the chunk really contains this document
                boolean containsDoc = shardKeyValue.compareTo(min) >= 0 && 
                                     shardKeyValue.compareTo(max) < 0;
                logger.debug("Document _id: {} is in chunk: shard={}, min={}, max={}. Verified: {}", 
                            id, chunk.getShard(), chunk.getMin(), chunk.getMax(), containsDoc);
                
                if (!containsDoc) {
                    logger.warn("Document with _id {} and shardKey {} appears to be outside chunk bounds: [{}, {})",
                               id, shardKeyValue, chunk.getMin(), chunk.getMax());
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
     * Extracts the shard key value from a document and wraps it in a BsonValueWrapper
     * 
     * @param shardKey Set of field names that make up the shard key
     * @param doc The document containing the shard key values
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
                    logger.warn("Missing shard key field {} in document with _id: {}", 
                              keyField, doc.get("_id"));
                    // Handle missing shard key field - could use a default value or throw an exception
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