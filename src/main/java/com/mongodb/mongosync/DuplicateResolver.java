package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardbalancer.CountingMegachunk;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonValueConverter;
import com.mongodb.util.bson.BsonValueWrapper;

public class DuplicateResolver {
	
	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);

	private final ShardClient destShardClient;

	// Maps namespace -> (shard key value -> List of chunks containing that shard key value)
	private Map<String, Map<Object, List<CountingMegachunk>>> shardKeyToChunksMap = new HashMap<>();

	// Maps namespace -> (chunk -> Set of duplicate shard key values in that chunk)
	private Map<String, Map<CountingMegachunk, Set<Object>>> chunkToDuplicateShardKeysMap = new HashMap<>();

	// Maps shard -> List of chunks that need splitting
	private Map<String, List<ChunkSplitInfo>> shardToSplitInfoMap = new HashMap<>();
	
	// Map of namespace -> Collection info document -- "key" is shard key
	private Map<String, Document> collectionsMap;

	public DuplicateResolver(ShardClient destShardClient) {
		this.destShardClient = destShardClient;
		this.collectionsMap = destShardClient.getCollectionsMap();
	}

	public void executeSplitsAndMigrations() {
	    logger.debug("executeSplitsAndMigrations");
	    
	    // Log duplicate mappings size
	    logger.debug("Processing {} namespaces with duplicate mappings", shardKeyToChunksMap.keySet().size());
	    
	    // Process each namespace
	    for (String namespace : shardKeyToChunksMap.keySet()) {
	        // First identify duplicate shard key values that need to be separated
	        Map<Object, List<CountingMegachunk>> duplicatesMap = shardKeyToChunksMap.get(namespace);
	        logger.debug("Namespace: {} has {} duplicate shard key values to process", namespace, duplicatesMap.size());
	        
	        int dupeCount = 0;
	        // For each duplicate shard key value
	        for (Map.Entry<Object, List<CountingMegachunk>> entry : duplicatesMap.entrySet()) {
	            Object shardKeyValue = entry.getKey();
	            List<CountingMegachunk> chunks = entry.getValue();

	            // Skip if this shard key value is only in one chunk
	            if (chunks.size() <= 1)
	                continue;
	            
	            dupeCount++;
	            if (dupeCount <= 5) {  // Log first 5 duplicates for diagnostics
	                logger.debug("Duplicate shard key value: {} appears in {} chunks", shardKeyValue, chunks.size());
	                for (CountingMegachunk chunk : chunks) {
	                    logger.debug("  - Chunk on shard: {}, min: {}, max: {}", 
	                                chunk.getShard(), chunk.getMin(), chunk.getMax());
	                }
	            }

	            // Group chunks by shard
	            Map<String, List<CountingMegachunk>> chunksByShardMap = new HashMap<>();
	            for (CountingMegachunk chunk : chunks) {
	                String shardId = chunk.getShard();
	                chunksByShardMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(chunk);
	            }
	            
	            // Log chunk distribution across shards
	            if (dupeCount <= 5) {
	                logger.debug("Duplicate shard key value: {} distribution across shards:", shardKeyValue);
	                for (Map.Entry<String, List<CountingMegachunk>> shardEntry : chunksByShardMap.entrySet()) {
	                    logger.debug("  - Shard: {} has {} chunks with this shard key value", 
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
	                    handleChunksInSameShard(namespace, shardKeyValue, shardId, chunksInShard);
	                }
	            }
	        }
	        logger.debug("Total of {} duplicate shard key values processed for namespace {}", dupeCount, namespace);
	    }
	}
	
	/**
	 * Check if a chunk truly contains a document based on its shard key
	 */
	private boolean chunkContainsDocument(CountingMegachunk chunk, Document doc, Set<String> shardKey) {
	    BsonValueWrapper docValue = getShardKeyWrapper(shardKey, doc);
	    BsonValueWrapper minValue = new BsonValueWrapper(chunk.getMin());
	    BsonValueWrapper maxValue = new BsonValueWrapper(chunk.getMax());
	    
	    // Check if docValue is within the chunk's range (inclusive of min, exclusive of max)
	    boolean result = (docValue.compareTo(minValue) >= 0 && docValue.compareTo(maxValue) < 0);
	    logger.debug("Checking if document with shard key={} is in chunk: min={}, max={}. Result: {}", 
	                docValue, minValue, maxValue, result);
	    return result;
	}

	private void handleChunksInSameShard(String namespace, Object shardKeyValue, String sourceShardId,
			List<CountingMegachunk> chunksInShard) {
		logger.info("Handling {} chunks in shard {} with duplicate shard key value: {}", 
                    chunksInShard.size(), sourceShardId, shardKeyValue);

		// For each chunk except the first one, try to move it to a different shard
		for (int i = 1; i < chunksInShard.size(); i++) {
			CountingMegachunk chunk = chunksInShard.get(i);

			// Check if we need to split this chunk first
			if (chunkToDuplicateShardKeysMap.containsKey(namespace)
					&& chunkToDuplicateShardKeysMap.get(namespace).containsKey(chunk)
					&& chunkToDuplicateShardKeysMap.get(namespace).get(chunk).size() > 1) {

				// This chunk contains multiple duplicate shard key values, so we should split it first
				splitChunkForMultipleDuplicates(namespace, chunk, chunkToDuplicateShardKeysMap.get(namespace).get(chunk));
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

	private void splitChunkForMultipleDuplicates(String namespace, CountingMegachunk chunk, Set<Object> duplicateShardKeyValues) {
	    // Get the shard key fields for this namespace
	    Document collMeta = collectionsMap.get(namespace);
	    if (collMeta == null) {
	        logger.error("No collection metadata found for namespace: {}", namespace);
	        return;
	    }
	    Document shardKeyDoc = (Document)collMeta.get("key");
	    Set<String> shardKeyFields = shardKeyDoc.keySet();
	    
	    // Convert duplicate shard key values to a sorted list
	    List<Object> sortedShardKeyValues = new ArrayList<>(duplicateShardKeyValues);
	    Collections.sort(sortedShardKeyValues, (a, b) -> a.toString().compareTo(b.toString()));

	    logger.info("Preparing to split chunk for namespace {} with {} duplicate shard key values", 
                   namespace, sortedShardKeyValues.size());
	    logger.debug("Chunk details: shard={}, min={}, max={}", 
                    chunk.getShard(), chunk.getMin(), chunk.getMax());
	    
	    // Log the first few shard key values
	    for (int i = 0; i < Math.min(sortedShardKeyValues.size(), 5); i++) {
	        logger.debug("Sample duplicate shard key value {}: {}", i, sortedShardKeyValues.get(i));
	    }

	    // We'll split at every nth duplicate shard key value to keep the number of splits manageable
	    int splitInterval = Math.max(1, sortedShardKeyValues.size() / 3); // Don't create too many splits
	    logger.debug("Using split interval of {} (will create approximately {} splits)", 
	                splitInterval, sortedShardKeyValues.size() / splitInterval);

	    for (int i = splitInterval; i < sortedShardKeyValues.size(); i += splitInterval) {
	        Object splitPointShardKeyValue = sortedShardKeyValues.get(i);

	        // Create a document for the split point
	        Document splitPoint = new Document();
	        
	        // If there's only one field in the shard key, it's simple
	        if (shardKeyFields.size() == 1) {
	            String keyField = shardKeyFields.iterator().next();
	            splitPoint.append(keyField, splitPointShardKeyValue);
	        } else {
	            // For compound shard keys, we need to extract each component
	            // This assumes the splitPointShardKeyValue is a BsonDocument or a structure
	            // that can be converted to one
	            if (splitPointShardKeyValue instanceof BsonDocument) {
	                BsonDocument bsonShardKey = (BsonDocument) splitPointShardKeyValue;
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
	    shardKeyToChunksMap.putIfAbsent(namespace, new HashMap<>());
	    chunkToDuplicateShardKeysMap.putIfAbsent(namespace, new HashMap<>());
	    
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
	        // Extract shard key value
	        BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKey, doc);
	        Object id = doc.get("_id");  // Still need _id for logging
	        
	        processedCount++;
	        if (processedCount <= 5) {
	            logger.debug("Processing duplicate document _id: {}, shardKey: {}", id, shardKeyValue);
	        }

	        // Find which chunk this document belongs to
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
	                logger.warn("Document with shardKey {} appears to be outside chunk bounds: [{}, {})",
	                           shardKeyValue, chunk.getMin(), chunk.getMax());
	            }
	        }

	        // Update shardKey -> chunks mapping
	        shardKeyToChunksMap.get(namespace).computeIfAbsent(shardKeyValue, k -> new ArrayList<>()).add(chunk);

	        // Update chunk -> duplicate shard key values mapping
	        chunkToDuplicateShardKeysMap.get(namespace).computeIfAbsent(chunk, k -> new HashSet<>()).add(shardKeyValue);
	    }
	    
	    logger.debug("Finished building duplicate mapping. Processed {} documents, encountered {} errors.",
	                processedCount, errorCount);
	                
	    // Log some stats about the mappings
	    int shardKeysWithMultipleChunks = 0;
	    for (Map.Entry<Object, List<CountingMegachunk>> entry : shardKeyToChunksMap.get(namespace).entrySet()) {
	        if (entry.getValue().size() > 1) {
	            shardKeysWithMultipleChunks++;
	        }
	    }
	    logger.debug("Found {} shard key values that appear in multiple chunks", shardKeysWithMultipleChunks);
	    
	    logger.debug("Chunks with duplicate shard key values: {}", chunkToDuplicateShardKeysMap.get(namespace).size());
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

	public void determineSplitPoints(String namespace) {
		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
		    logger.error("No collection metadata found for namespace: {}", namespace);
		    return;
		}
		Document shardKeyDoc = (Document)collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();
		
		Map<Object, List<CountingMegachunk>> shardKeyMap = shardKeyToChunksMap.get(namespace);
		Map<CountingMegachunk, Set<Object>> chunkMap = chunkToDuplicateShardKeysMap.get(namespace);

		// Group duplicate shard key values by chunk and shard
		Map<String, Map<CountingMegachunk, List<Object>>> shardToChunksWithDupes = new HashMap<>();

		// Find shard key values that appear in multiple chunks
		for (Map.Entry<Object, List<CountingMegachunk>> entry : shardKeyMap.entrySet()) {
			if (entry.getValue().size() > 1) {
				Object shardKeyValue = entry.getKey();
				// Group by shard
				for (CountingMegachunk chunk : entry.getValue()) {
					String shard = chunk.getShard();
					shardToChunksWithDupes.computeIfAbsent(shard, k -> new HashMap<>())
							.computeIfAbsent(chunk, k -> new ArrayList<>()).add(shardKeyValue);
				}
			}
		}

		// For each shard, determine split points for chunks with duplicates
		for (Map.Entry<String, Map<CountingMegachunk, List<Object>>> shardEntry : shardToChunksWithDupes.entrySet()) {
			String shard = shardEntry.getKey();
			Map<CountingMegachunk, List<Object>> chunksWithDupes = shardEntry.getValue();

			for (Map.Entry<CountingMegachunk, List<Object>> chunkEntry : chunksWithDupes.entrySet()) {
				CountingMegachunk chunk = chunkEntry.getKey();
				List<Object> dupeShardKeyValues = chunkEntry.getValue();

				// Sort duplicate shard key values (assuming they're comparable)
				Collections.sort(dupeShardKeyValues, (a, b) -> a.toString().compareTo(b.toString()));

				if (dupeShardKeyValues.size() > 1) {
					// Add split points between clusters of duplicate shard key values
					List<Object> splitPoints = calculateSplitPoints(dupeShardKeyValues);

					// Record chunk split info
					ChunkSplitInfo splitInfo = new ChunkSplitInfo(namespace, chunk, splitPoints);

					shardToSplitInfoMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(splitInfo);
				}
			}
		}
	}

	// Calculate optimal split points to separate clusters of duplicate shard key values
	private List<Object> calculateSplitPoints(List<Object> sortedShardKeyValues) {
		List<Object> splitPoints = new ArrayList<>();

		// Group shard key values by closeness
		List<List<Object>> clusters = clusterShardKeyValues(sortedShardKeyValues);

		// Add a split point between each cluster
		for (int i = 0; i < clusters.size() - 1; i++) {
			List<Object> cluster = clusters.get(i);
			List<Object> nextCluster = clusters.get(i + 1);

			Object lastShardKey = cluster.get(cluster.size() - 1);
			Object firstShardKeyOfNextCluster = nextCluster.get(0);

			// Create a split point between clusters
			// This could be something between lastShardKey and firstShardKeyOfNextCluster
			Object splitPoint = calculateMidpoint(lastShardKey, firstShardKeyOfNextCluster);
			splitPoints.add(splitPoint);
		}

		return splitPoints;
	}

	// Simple clustering algorithm - group shard key values that are "close" to each other
	private List<List<Object>> clusterShardKeyValues(List<Object> sortedShardKeyValues) {
		List<List<Object>> clusters = new ArrayList<>();
		if (sortedShardKeyValues.isEmpty())
			return clusters;

		List<Object> currentCluster = new ArrayList<>();
		currentCluster.add(sortedShardKeyValues.get(0));
		clusters.add(currentCluster);

		for (int i = 1; i < sortedShardKeyValues.size(); i++) {
			Object currentShardKey = sortedShardKeyValues.get(i);
			Object previousShardKey = sortedShardKeyValues.get(i - 1);

			// If current shard key value is "far" from previous, start a new cluster
			if (isSignificantGap(previousShardKey, currentShardKey)) {
				currentCluster = new ArrayList<>();
				clusters.add(currentCluster);
			}

			currentCluster.add(currentShardKey);
		}

		return clusters;
	}

	// Determine if there's a significant gap between shard key values
	private boolean isSignificantGap(Object shardKey1, Object shardKey2) {
		// This is a simplistic implementation
		// In practice, you'd need logic specific to your shard key type
		if (shardKey1 instanceof Number && shardKey2 instanceof Number) {
			double n1 = ((Number) shardKey1).doubleValue();
			double n2 = ((Number) shardKey2).doubleValue();
			return Math.abs(n2 - n1) > 100; // Arbitrary threshold
		}

		// For string shard keys, could check lexicographic distance
		return false;
	}

	// Calculate a value between shardKey1 and shardKey2 to use as split point
	private Object calculateMidpoint(Object shardKey1, Object shardKey2) {
		// Implementation depends on the type of your shard key
		if (shardKey1 instanceof Integer && shardKey2 instanceof Integer) {
			return (Integer) shardKey1 + ((Integer) shardKey2 - (Integer) shardKey1) / 2;
		} else if (shardKey1 instanceof Long && shardKey2 instanceof Long) {
			return (Long) shardKey1 + ((Long) shardKey2 - (Long) shardKey1) / 2;
		} else if (shardKey1 instanceof String && shardKey2 instanceof String) {
			// For string shard keys, you might need a different approach
			return shardKey1;
		}

		// Default fallback
		return shardKey1;
	}

	// Execute the splits and migrations
	public void executeSplitsAndMigrations(ShardClient client) {
		// First perform all splits
		for (Map.Entry<String, List<ChunkSplitInfo>> entry : shardToSplitInfoMap.entrySet()) {
			for (ChunkSplitInfo splitInfo : entry.getValue()) {
				for (Object splitPoint : splitInfo.getSplitPoints()) {
					// Convert splitPoint to appropriate format for MongoDB command
					Document splitPointDoc = convertToSplitPointFormat(splitInfo.getNamespace(), splitPoint);

					// Execute split
					client.splitChunk(splitInfo.getNamespace(), splitInfo.getChunk().getMin(),
							splitInfo.getChunk().getMax(), splitPointDoc);
				}
			}
		}

		// Then perform migrations to separate duplicates
		migrateSplitChunks(client);
	}

	private void migrateSplitChunks(ShardClient client) {
		// Logic to migrate split chunks to different shards
		// This would need to be implemented based on your specific needs
	}

	private Document convertToSplitPointFormat(String namespace, Object splitPoint) {
	    // Get the shard key for this namespace
	    Document collMeta = collectionsMap.get(namespace);
	    if (collMeta == null) {
	        logger.error("No collection metadata found for namespace: {}", namespace);
	        return new Document("_id", splitPoint); // Fallback
	    }
	    
	    Document shardKeyDoc = (Document)collMeta.get("key");
	    Set<String> shardKeyFields = shardKeyDoc.keySet();
	    
	    Document splitPointDoc = new Document();
	    
	    // Handle different types of split points based on shard key
	    if (shardKeyFields.size() == 1) {
	        // Single field shard key
	        String keyField = shardKeyFields.iterator().next();
	        splitPointDoc.append(keyField, splitPoint);
	    } else if (splitPoint instanceof BsonDocument) {
	        // Compound shard key
	        BsonDocument bsonSplitPoint = (BsonDocument) splitPoint;
	        for (String field : shardKeyFields) {
	            if (bsonSplitPoint.containsKey(field)) {
	                BsonValue fieldValue = bsonSplitPoint.get(field);
	                splitPointDoc.append(field, BsonValueConverter.convertBsonValueToObject(fieldValue));
	            }
	        }
	    } else {
	        logger.warn("Cannot create proper split point document from: {}", splitPoint);
	        // Fallback to using it as is
	        return new Document("_id", splitPoint);
	    }
	    
	    return splitPointDoc;
	}

	// Helper class to store information about chunks that need splitting
	private static class ChunkSplitInfo {
		private final String namespace;
		private final CountingMegachunk chunk;
		private final List<Object> splitPoints;

		public ChunkSplitInfo(String namespace, CountingMegachunk chunk, List<Object> splitPoints) {
			this.namespace = namespace;
			this.chunk = chunk;
			this.splitPoints = splitPoints;
		}

		public String getNamespace() {
			return namespace;
		}

		public CountingMegachunk getChunk() {
			return chunk;
		}

		public List<Object> getSplitPoints() {
			return splitPoints;
		}
	}
}