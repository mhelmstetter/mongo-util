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

	// Maps namespace -> (_id -> List of chunks containing that _id)
	private Map<String, Map<Object, List<CountingMegachunk>>> idToChunksMap = new HashMap<>();

	// Maps namespace -> (chunk -> Set of duplicate _ids in that chunk)
	private Map<String, Map<CountingMegachunk, Set<Object>>> chunkToDuplicateIdsMap = new HashMap<>();

	// Maps shard -> List of chunks that need splitting
	private Map<String, List<ChunkSplitInfo>> shardToSplitInfoMap = new HashMap<>();

	public DuplicateResolver(ShardClient destShardClient) {
		this.destShardClient = destShardClient;
	}

	/**
	 * Execute the splits and migrations
	 */
	public void executeSplitsAndMigrations() {
		
		logger.debug("executeSplitsAndMigrations");
		
		// Process each namespace
		for (String namespace : idToChunksMap.keySet()) {
			// First identify duplicate IDs that need to be separated
			Map<Object, List<CountingMegachunk>> duplicatesMap = idToChunksMap.get(namespace);

			// For each duplicate ID
			for (Map.Entry<Object, List<CountingMegachunk>> entry : duplicatesMap.entrySet()) {
				Object id = entry.getKey();
				List<CountingMegachunk> chunks = entry.getValue();

				// Skip if this ID is only in one chunk
				if (chunks.size() <= 1)
					continue;

				// Group chunks by shard
				Map<String, List<CountingMegachunk>> chunksByShardMap = new HashMap<>();
				for (CountingMegachunk chunk : chunks) {
					String shardId = chunk.getShard();
					chunksByShardMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(chunk);
				}

				// For each shard that has multiple chunks with this duplicate
				for (Map.Entry<String, List<CountingMegachunk>> shardEntry : chunksByShardMap.entrySet()) {
					String shardId = shardEntry.getKey();
					List<CountingMegachunk> chunksInShard = shardEntry.getValue();

					// If there's more than one chunk with this duplicate in the same shard
					if (chunksInShard.size() > 1) {
						// We need to migrate at least one of these chunks to a different shard
						handleChunksInSameShard(namespace, id, shardId, chunksInShard);
					}
				}
			}
		}
	}

	private void handleChunksInSameShard(String namespace, Object id, String sourceShardId,
			List<CountingMegachunk> chunksInShard) {
		logger.info("Handling {} chunks in shard {} with duplicate _id: {}", chunksInShard.size(), sourceShardId, id);

// For each chunk except the first one, try to move it to a different shard
		for (int i = 1; i < chunksInShard.size(); i++) {
			CountingMegachunk chunk = chunksInShard.get(i);

			// Check if we need to split this chunk first
			if (chunkToDuplicateIdsMap.containsKey(namespace)
					&& chunkToDuplicateIdsMap.get(namespace).containsKey(chunk)
					&& chunkToDuplicateIdsMap.get(namespace).get(chunk).size() > 1) {

				// This chunk contains multiple duplicate IDs, so we should split it first
				splitChunkForMultipleDuplicates(namespace, chunk, chunkToDuplicateIdsMap.get(namespace).get(chunk));
			}

			// Get a different target shard
			String targetShardId = findDifferentShard(sourceShardId);
			if (targetShardId != null) {


				logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {}", chunk.getMin(), chunk.getMax(),
						sourceShardId, targetShardId);

				// Use the existing moveChunk method
				boolean success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), targetShardId);

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

	private void splitChunkForMultipleDuplicates(String namespace, CountingMegachunk chunk, Set<Object> duplicateIds) {
		// Convert duplicate IDs to a sorted list
		List<Object> sortedIds = new ArrayList<>(duplicateIds);
		Collections.sort(sortedIds, (a, b) -> a.toString().compareTo(b.toString()));

		logger.info("Preparing to split chunk for namespace {} with {} duplicate IDs", namespace, sortedIds.size());

		// We'll split at every nth duplicate ID to keep the number of splits manageable
		int splitInterval = Math.max(1, sortedIds.size() / 3); // Don't create too many splits

		for (int i = splitInterval; i < sortedIds.size(); i += splitInterval) {
			Object splitPointId = sortedIds.get(i);

			// Create a document for the split point
			Document splitPoint = new Document("_id", splitPointId);

			logger.info("Attempting to split chunk at _id: {} for namespace {}", splitPointId, namespace);

			// Call the split command
			boolean success = destShardClient.splitChunk(namespace, chunk.getMin(), chunk.getMax(), splitPoint);

			if (success) {
				logger.info("Successfully split chunk at _id: {}", splitPointId);
			} else {
				logger.warn("Failed to split chunk at _id: {}", splitPointId);
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
		idToChunksMap.putIfAbsent(namespace, new HashMap<>());
		chunkToDuplicateIdsMap.putIfAbsent(namespace, new HashMap<>());

		// Process each duplicate document
		for (Document doc : duplicates) {
			Object id = doc.get("_id");
			BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKey, doc);

			// Find which chunk this document belongs to
			Map.Entry<BsonValueWrapper, CountingMegachunk> entry = chunkMap.floorEntry(shardKeyValue);
			if (entry == null)
				continue;

			CountingMegachunk chunk = entry.getValue();

			// Update id -> chunks mapping
			idToChunksMap.get(namespace).computeIfAbsent(id, k -> new ArrayList<>()).add(chunk);

			// Update chunk -> duplicate ids mapping
			chunkToDuplicateIdsMap.get(namespace).computeIfAbsent(chunk, k -> new HashSet<>()).add(id);
		}
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
		Map<Object, List<CountingMegachunk>> idMap = idToChunksMap.get(namespace);
		Map<CountingMegachunk, Set<Object>> chunkMap = chunkToDuplicateIdsMap.get(namespace);

		// Group duplicate ids by chunk and shard
		Map<String, Map<CountingMegachunk, List<Object>>> shardToChunksWithDupes = new HashMap<>();

		// Find ids that appear in multiple chunks
		for (Map.Entry<Object, List<CountingMegachunk>> entry : idMap.entrySet()) {
			if (entry.getValue().size() > 1) {
				Object id = entry.getKey();
				// Group by shard
				for (CountingMegachunk chunk : entry.getValue()) {
					String shard = chunk.getShard();
					shardToChunksWithDupes.computeIfAbsent(shard, k -> new HashMap<>())
							.computeIfAbsent(chunk, k -> new ArrayList<>()).add(id);
				}
			}
		}

		// For each shard, determine split points for chunks with duplicates
		for (Map.Entry<String, Map<CountingMegachunk, List<Object>>> shardEntry : shardToChunksWithDupes.entrySet()) {
			String shard = shardEntry.getKey();
			Map<CountingMegachunk, List<Object>> chunksWithDupes = shardEntry.getValue();

			for (Map.Entry<CountingMegachunk, List<Object>> chunkEntry : chunksWithDupes.entrySet()) {
				CountingMegachunk chunk = chunkEntry.getKey();
				List<Object> dupeIds = chunkEntry.getValue();

				// Sort duplicate ids (assuming they're comparable)
				Collections.sort(dupeIds, (a, b) -> a.toString().compareTo(b.toString()));

				if (dupeIds.size() > 1) {
					// Add split points between clusters of duplicate ids
					List<Object> splitPoints = calculateSplitPoints(dupeIds);

					// Record chunk split info
					ChunkSplitInfo splitInfo = new ChunkSplitInfo(namespace, chunk, splitPoints);

					shardToSplitInfoMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(splitInfo);
				}
			}
		}
	}

	// Calculate optimal split points to separate clusters of duplicate ids
	private List<Object> calculateSplitPoints(List<Object> sortedIds) {
		List<Object> splitPoints = new ArrayList<>();

		// Group ids by closeness
		List<List<Object>> clusters = clusterIds(sortedIds);

		// Add a split point between each cluster
		for (int i = 0; i < clusters.size() - 1; i++) {
			List<Object> cluster = clusters.get(i);
			List<Object> nextCluster = clusters.get(i + 1);

			Object lastId = cluster.get(cluster.size() - 1);
			Object firstIdOfNextCluster = nextCluster.get(0);

			// Create a split point between clusters
			// This could be something between lastId and firstIdOfNextCluster
			Object splitPoint = calculateMidpoint(lastId, firstIdOfNextCluster);
			splitPoints.add(splitPoint);
		}

		return splitPoints;
	}

	// Simple clustering algorithm - group ids that are "close" to each other
	private List<List<Object>> clusterIds(List<Object> sortedIds) {
		List<List<Object>> clusters = new ArrayList<>();
		if (sortedIds.isEmpty())
			return clusters;

		List<Object> currentCluster = new ArrayList<>();
		currentCluster.add(sortedIds.get(0));
		clusters.add(currentCluster);

		for (int i = 1; i < sortedIds.size(); i++) {
			Object currentId = sortedIds.get(i);
			Object previousId = sortedIds.get(i - 1);

			// If current id is "far" from previous, start a new cluster
			if (isSignificantGap(previousId, currentId)) {
				currentCluster = new ArrayList<>();
				clusters.add(currentCluster);
			}

			currentCluster.add(currentId);
		}

		return clusters;
	}

	// Determine if there's a significant gap between ids
	private boolean isSignificantGap(Object id1, Object id2) {
		// This is a simplistic implementation
		// In practice, you'd need logic specific to your _id type
		if (id1 instanceof Number && id2 instanceof Number) {
			double n1 = ((Number) id1).doubleValue();
			double n2 = ((Number) id2).doubleValue();
			return Math.abs(n2 - n1) > 100; // Arbitrary threshold
		}

		// For string ids, could check lexicographic distance
		return false;
	}

	// Calculate a value between id1 and id2 to use as split point
	private Object calculateMidpoint(Object id1, Object id2) {
		// Implementation depends on the type of your _id
		if (id1 instanceof Integer && id2 instanceof Integer) {
			return (Integer) id1 + ((Integer) id2 - (Integer) id1) / 2;
		} else if (id1 instanceof Long && id2 instanceof Long) {
			return (Long) id1 + ((Long) id2 - (Long) id1) / 2;
		} else if (id1 instanceof String && id2 instanceof String) {
			// For string ids, you might need a different approach
			return id1;
		}

		// Default fallback
		return id1;
	}

	// Execute the splits and migrations
	public void executeSplitsAndMigrations(ShardClient client) {
		// First perform all splits
		for (Map.Entry<String, List<ChunkSplitInfo>> entry : shardToSplitInfoMap.entrySet()) {
			for (ChunkSplitInfo splitInfo : entry.getValue()) {
				for (Object splitPoint : splitInfo.getSplitPoints()) {
					// Convert splitPoint to appropriate format for MongoDB command
					Document splitPointDoc = convertToSplitPointFormat(splitPoint);

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

	private Document convertToSplitPointFormat(Object splitPoint) {
		// Convert the split point object to the format expected by MongoDB's split
		// command
		// Implementation depends on your data types
		return new Document("_id", splitPoint);
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
