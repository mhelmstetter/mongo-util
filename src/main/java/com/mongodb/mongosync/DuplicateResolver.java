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
	
	private ChunkManager chunkManager;

	public DuplicateResolver(ShardClient destShardClient, ChunkManager chunkManager) {
		this.destShardClient = destShardClient;
		this.collectionsMap = destShardClient.getCollectionsMap();

		chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
		this.chunkManager = chunkManager;
	}
	
	public void executeSplitsAndMigrations() {
	    logger.debug("executeSplitsAndMigrations: processing {} namespaces with duplicate _id mappings", duplicateIdToDocsMap.keySet().size());
	    
	    // Create a set to track which chunks we've already processed
	    Set<String> processedChunkIdentifiers = new HashSet<>();
	    
	    boolean splitsPerformed = false;
	    
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
	        
	        // First identify duplicate _ids that need to be separated
	        Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
	        logger.debug("Namespace: {} has {} duplicate _id values to process", namespace, duplicatesMap.size());
	        
	        // Group chunks by shard and process each chunk once
	        
	        // Maps shard -> (chunk -> list of duplicate IDs in that chunk)
	        Map<String, Map<CountingMegachunk, Set<Object>>> shardToChunksMap = new HashMap<>();
	        
	        // For each duplicate _id, determine which chunks it's in
	        for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
	            Object duplicateId = entry.getKey();
	            List<Document> docsWithSameId = entry.getValue();
	            
	            // Skip if not a duplicate
	            if (docsWithSameId.size() <= 1) continue;
	            
	            // Map to track which chunk each document with this duplicate _id belongs to
	            Map<CountingMegachunk, Document> chunkToDocMap = new HashMap<>();
	            
	            // Find which chunk each document belongs to
	            for (Document doc : docsWithSameId) {
	                BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);
	                
	                NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                if (chunkMap == null) continue;
	                
	                Map.Entry<BsonValueWrapper, CountingMegachunk> entry1 = chunkMap.floorEntry(shardKeyValue);
	                if (entry1 == null) continue;
	                
	                CountingMegachunk chunk = entry1.getValue();
	                chunkToDocMap.put(chunk, doc);
	                
	                // Group by shard and chunk
	                String shardId = chunk.getShard();
	                shardToChunksMap
	                    .computeIfAbsent(shardId, k -> new HashMap<>())
	                    .computeIfAbsent(chunk, k -> new HashSet<>())
	                    .add(duplicateId);
	            }
	        }
	        
	        // Now process each shard and its chunks
	        for (Map.Entry<String, Map<CountingMegachunk, Set<Object>>> shardEntry : shardToChunksMap.entrySet()) {
	            String shardId = shardEntry.getKey();
	            Map<CountingMegachunk, Set<Object>> chunksInShard = shardEntry.getValue();
	            
	            logger.info("Processing shard {} with {} chunks containing duplicates", 
	                      shardId, chunksInShard.size());
	            
	            // Handle cases where multiple chunks on the same shard have the same duplicate IDs
	            // First, identify chunks that share duplicate IDs
	            Map<Object, List<CountingMegachunk>> idToChunksMap = new HashMap<>();
	            
	            for (Map.Entry<CountingMegachunk, Set<Object>> chunkEntry : chunksInShard.entrySet()) {
	                CountingMegachunk chunk = chunkEntry.getKey();
	                Set<Object> dupeIds = chunkEntry.getValue();
	                
	                for (Object dupeId : dupeIds) {
	                    idToChunksMap.computeIfAbsent(dupeId, k -> new ArrayList<>()).add(chunk);
	                }
	            }
	            
	            // Now find chunks that need to be moved (having the same duplicate IDs)
	            Set<CountingMegachunk> chunksToMove = new HashSet<>();
	            
	            for (List<CountingMegachunk> chunksWithSameId : idToChunksMap.values()) {
	                if (chunksWithSameId.size() > 1) {
	                    // Keep the first chunk, mark others for movement
	                    for (int i = 1; i < chunksWithSameId.size(); i++) {
	                        chunksToMove.add(chunksWithSameId.get(i));
	                    }
	                }
	            }
	            
	            // Now move each chunk that needs to be moved
	            for (CountingMegachunk chunkToMove : chunksToMove) {
	                String chunkId = chunkToMove.getShard() + "_" + chunkToMove.getMin() + "_" + chunkToMove.getMax();
	                
	                // Skip if already processed
	                if (processedChunkIdentifiers.contains(chunkId)) {
	                    logger.trace("Skipping already processed chunk: {}", chunkId);
	                    continue;
	                }
	                
	                // Proceed with migration
	                if (!chunkExistsWithBoundaries(namespace, chunkToMove.getMin(), chunkToMove.getMax())) {
	                    logger.warn("Chunk with bounds min: {}, max: {} no longer exists, skipping migration", 
	                               chunkToMove.getMin(), chunkToMove.getMax());
	                    processedChunkIdentifiers.add(chunkId);
	                    continue;
	                }
	                
	                String targetShardId = findDifferentShard(shardId);
	                if (targetShardId == null) {
	                    logger.error("Could not find a different shard to move chunk to from shard {}", shardId);
	                    continue;
	                }
	                
	                logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {}", 
	                          chunkToMove.getMin(), chunkToMove.getMax(), shardId, targetShardId);
	                
	                boolean success = false;
	                int retryCount = 0;
	                int sleep = 30000;
	                final int MAX_RETRIES = 3;
	                
	                while (!success && retryCount < MAX_RETRIES) {
	                    try {
	                        success = destShardClient.moveChunk(namespace, chunkToMove.getMin(), chunkToMove.getMax(), 
	                                                          targetShardId, false, false, true, false, false);
	                        
	                        if (success) {
	                        	Thread.sleep(1000);
	                            logger.info("Successfully moved chunk to shard {}", targetShardId);
	                            chunkToMove.setShard(targetShardId);
	                            processedChunkIdentifiers.add(chunkId);
	                        } else {
	                        	Thread.sleep(sleep);
	                            retryCount++;
	                            sleep = sleep * retryCount;
	                        }
	                    } catch (Exception e) {
	                    	try {
								Thread.sleep(sleep);
							} catch (InterruptedException e1) {
							}
	                        retryCount++;
	                        sleep = sleep * retryCount;
	                    }
	                }
	            }
	        }
	        
	        logger.debug("Completed processing chunks with duplicates for namespace {}", namespace);
	    }
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

	private void handleChunksInSameShard(String namespace, Object duplicateId, String sourceShardId,
	        List<CountingMegachunk> chunksInShard, Map<CountingMegachunk, List<Document>> chunkToDocsMap,
	        Set<String> processedChunkIdentifiers) {
	    
	    logger.info("Handling {} chunks in shard {} with duplicate _id value: {}", 
	                chunksInShard.size(), sourceShardId, duplicateId);

	    // Move only ONE chunk instead of all but the first one
	    if (chunksInShard.size() > 1) {
	        CountingMegachunk chunkToMove = chunksInShard.get(1);
	        
	        // Create a unique identifier for this chunk
	        String chunkId = chunkToMove.getShard() + "_" + chunkToMove.getMin() + "_" + chunkToMove.getMax();
	        
	        // Skip if we've already processed this chunk
	        if (processedChunkIdentifiers.contains(chunkId)) {
	            logger.debug("Skipping already processed chunk: {}", chunkId);
	            return;
	        }

	        // Check if we need to split this chunk first
	        if (chunkToDuplicateIdsMap.containsKey(namespace)
	                && chunkToDuplicateIdsMap.get(namespace).containsKey(chunkToMove)
	                && chunkToDuplicateIdsMap.get(namespace).get(chunkToMove).size() > 1) {

	            // This chunk contains multiple duplicate _id values, so we should split it first
	            List<Document> docsInChunk = chunkToDocsMap.get(chunkToMove);
	            if (docsInChunk != null && docsInChunk.size() > 1) {
	                logger.info("Chunk contains multiple duplicate _ids, attempting to split first");
	                splitChunkForMultipleDuplicates(namespace, chunkToMove, docsInChunk);
	                
	                // After splitting, refresh our chunk information before continuing
	                refreshChunkCache(namespace);
	                
	                // Since we've split the chunk, we need to get an updated view of the chunks
	                // Rather than continue with potentially stale data, we'll return and let 
	                // the next iteration handle the migrations with fresh chunk data
	                logger.info("Chunk was split, refreshing chunk information before migrations");
	                return;
	            }
	        }

	        // Get a different target shard
	        String targetShardId = findDifferentShard(sourceShardId);
	        if (targetShardId != null) {
	            // Before trying to move a chunk, verify it still exists with the current boundaries
	            if (!chunkExistsWithBoundaries(namespace, chunkToMove.getMin(), chunkToMove.getMax())) {
	                logger.warn("Chunk with bounds min: {}, max: {} no longer exists, skipping migration", 
	                           chunkToMove.getMin(), chunkToMove.getMax());
	                processedChunkIdentifiers.add(chunkId);
	                return;
	            }
	            
	            logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {}", 
	                        chunkToMove.getMin(), chunkToMove.getMax(), sourceShardId, targetShardId);

	            // Use the existing moveChunk method with proper error handling
	            boolean success = false;
	            int retryCount = 0;
	            final int MAX_RETRIES = 3;
	            
	            while (!success && retryCount < MAX_RETRIES) {
	                try {
	                    success = destShardClient.moveChunk(namespace, chunkToMove.getMin(), chunkToMove.getMax(), 
	                                                      targetShardId, false, false, true, false, false);
	                    
	                    if (success) {
	                        logger.info("Successfully moved chunk to shard {}", targetShardId);
	                        
	                        // Update the chunk's shard in our records
	                        chunkToMove.setShard(targetShardId);
	                        
	                        // Mark this chunk as processed
	                        processedChunkIdentifiers.add(chunkId);
	                    } else {
	                        retryCount++;
	                        logger.error("Failed to move chunk to shard {} (attempt {}/{})", 
	                                    targetShardId, retryCount, MAX_RETRIES);
	                        
	                        if (retryCount < MAX_RETRIES) {
	                            // Wait a bit before retrying
	                            try {
	                                Thread.sleep(1000 * retryCount);
	                            } catch (InterruptedException e) {
	                                Thread.currentThread().interrupt();
	                            }
	                            
	                            // Try a different target shard on retry
	                            targetShardId = findDifferentShard(sourceShardId);
	                            if (targetShardId == null) {
	                                logger.error("No alternative target shards available, giving up");
	                                break;
	                            }
	                            
	                            // Refresh chunk cache before retrying
	                            refreshChunkCache(namespace);
	                            
	                            // Verify chunk still exists before retrying
	                            if (!chunkExistsWithBoundaries(namespace, chunkToMove.getMin(), chunkToMove.getMax())) {
	                                logger.warn("Chunk with bounds min: {}, max: {} no longer exists, skipping migration", 
	                                           chunkToMove.getMin(), chunkToMove.getMax());
	                                processedChunkIdentifiers.add(chunkId);
	                                break;
	                            }
	                        }
	                    }
	                } catch (Exception e) {
	                    retryCount++;
	                    logger.error("Exception when moving chunk to shard {} (attempt {}/{}): {}", 
	                                targetShardId, retryCount, MAX_RETRIES, e.getMessage());
	                    
	                    if (retryCount >= MAX_RETRIES) {
	                        logger.error("Giving up on moving chunk after {} failed attempts", MAX_RETRIES);
	                    }
	                }
	            }
	        } else {
	            logger.error("Could not find a different shard to move chunk to from shard {}", sourceShardId);
	        }
	    } else {
	        logger.info("No need to move chunks as there is only one chunk with this duplicate ID in shard {}", 
	                   sourceShardId);
	    }
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
	            
	            // Extract the BSON value and convert it properly
	            BsonValue bsonValue = splitPointShardKeyValue.getValue();
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
	            if (splitPointShardKeyValue.getValue() instanceof BsonDocument) {
	                BsonDocument bsonShardKey = (BsonDocument) splitPointShardKeyValue.getValue();
	                for (String field : shardKeyFields) {
	                    if (bsonShardKey.containsKey(field)) {
	                        BsonValue fieldValue = bsonShardKey.get(field);
	                        Object keyValue = BsonValueConverter.convertBsonValueToObject(fieldValue);
	                        splitPoint.append(field, keyValue);
	                    }
	                }
	            } else {
	                logger.warn("Cannot create split point for compound shard key from: {}", splitPointShardKeyValue);
	                continue;
	            }
	        }

	        logger.info("Attempting to split chunk at shard key value: {} with split point: {}", 
	                   splitPointShardKeyValue, splitPoint);

	        try {
	            // Call the split command with proper error handling
	            boolean success = destShardClient.splitChunk(namespace, chunk.getMin(), chunk.getMax(), splitPoint);

	            if (success) {
	                logger.info("Successfully split chunk at shard key value: {}", splitPointShardKeyValue);
	            } else {
	                logger.warn("Failed to split chunk at shard key value: {}", splitPointShardKeyValue);
	            }
	        } catch (Exception e) {
	            logger.error("Error while splitting chunk at shard key value: {}: {}", 
	                       splitPointShardKeyValue, e.getMessage());
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
				// Verify the chunk really contains this document
				boolean containsDoc = shardKeyValue.compareTo(min) >= 0 && shardKeyValue.compareTo(max) < 0;
				logger.debug("Document _id: {} is in chunk: shard={}, min={}, max={}. Verified: {}", id,
						chunk.getShard(), chunk.getMin(), chunk.getMax(), containsDoc);

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