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

	private Set<Object> cycleDetectedIds = new HashSet<>();

	private final Map<String, RawBsonDocument> destChunksCache = new LinkedHashMap<>();
	private final Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> destChunkMap = new HashMap<>();

	private ChunkManager chunkManager;
	private String archiveDbName;

	/**
	 * Class to store categorized conflicts for optimized resolution
	 */
	private static class ConflictCategories {
		// Simple conflicts that can be resolved with basic operations
		private final Map<String, List<Object>> simpleConflicts;

		// Complex conflicts that require more advanced operations
		private final Map<String, List<Object>> complexConflicts;

		// Special case conflicts that need specific handling
		private final Map<String, List<Object>> specialCaseConflicts;

		public ConflictCategories(Map<String, List<Object>> simpleConflicts, Map<String, List<Object>> complexConflicts,
				Map<String, List<Object>> specialCaseConflicts) {
			this.simpleConflicts = simpleConflicts;
			this.complexConflicts = complexConflicts;
			this.specialCaseConflicts = specialCaseConflicts;
		}

		public Map<String, List<Object>> getSimpleConflicts() {
			return simpleConflicts;
		}

		public Map<String, List<Object>> getComplexConflicts() {
			return complexConflicts;
		}

		public Map<String, List<Object>> getSpecialCaseConflicts() {
			return specialCaseConflicts;
		}

		/**
		 * Get all IDs that need conflict resolution
		 */
		public Set<Object> getAllConflictIds() {
			Set<Object> allIds = new HashSet<>();

			// Add all simple conflict IDs
			for (List<Object> ids : simpleConflicts.values()) {
				allIds.addAll(ids);
			}

			// Add all complex conflict IDs
			for (List<Object> ids : complexConflicts.values()) {
				allIds.addAll(ids);
			}

			// Add all special case conflict IDs
			for (List<Object> ids : specialCaseConflicts.values()) {
				allIds.addAll(ids);
			}

			return allIds;
		}

		/**
		 * Get IDs with negative shard keys
		 */
		public List<Object> getNegativeKeyIds() {
			return specialCaseConflicts.getOrDefault("negativeKeys", Collections.emptyList());
		}

		/**
		 * Get IDs suitable for chunk splitting
		 */
		public List<Object> getSplitCandidateIds() {
			return complexConflicts.getOrDefault("wideDistribution", Collections.emptyList());
		}
	}

	public DuplicateResolver(ShardClient destShardClient, ChunkManager chunkManager, String archiveDbName) {
		this.destShardClient = destShardClient;
		this.collectionsMap = destShardClient.getCollectionsMap();
		this.archiveDbName = archiveDbName;

		chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
		this.chunkManager = chunkManager;
	}

	/**
	 * Improved verification method that uses fresh chunk data
	 */
	private boolean verifyNoConflictsRemain(String namespace, Map<Document, CountingMegachunk> docToChunkMap,
			Map<Object, List<Document>> duplicatesMap) {
		logger.info("Verifying no conflicts remain for namespace: {}", namespace);

		// Refresh chunk cache before verification
		refreshChunkCache(namespace);

		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
			logger.error("No collection metadata found for namespace: {}", namespace);
			return false;
		}
		Document shardKeyDoc = (Document) collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();

		// Create a fresh document-to-chunk mapping
		Map<Document, CountingMegachunk> freshDocToChunkMap = new HashMap<>();

		// Rebuild the document to chunk mapping with fresh chunk data
		for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
			for (Document doc : entry.getValue()) {
				try {
					BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
					NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);

					if (chunkMap != null) {
						Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap
								.floorEntry(shardKeyWrapper);
						if (chunkEntry != null) {
							freshDocToChunkMap.put(doc, chunkEntry.getValue());
						}
					}
				} catch (Exception e) {
					logger.error("Error updating chunk for doc with _id {}: {}", doc.get("_id"), e.getMessage());
				}
			}
		}

		int conflictCount = 0;

		for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
			Object id = entry.getKey();
			List<Document> docsWithSameId = entry.getValue();

			if (docsWithSameId.size() <= 1)
				continue;

			// Group by shard using fresh chunk mapping
			Map<String, List<Document>> shardToDocsMap = new HashMap<>();

			for (Document doc : docsWithSameId) {
				CountingMegachunk chunk = freshDocToChunkMap.get(doc);
				if (chunk == null) {
					logger.warn("Missing chunk mapping for doc with _id {} during verification", id);
					continue;
				}

				String shard = chunk.getShard();
				shardToDocsMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(doc);
			}

			// Check if any shard has multiple documents with this ID
			for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
				if (shardEntry.getValue().size() > 1) {
					conflictCount++;
					String shardId = shardEntry.getKey();
					List<Document> conflictDocs = shardEntry.getValue();

					// Log detailed info about the conflict
					StringBuilder details = new StringBuilder();
					String primaryShardKey = shardKeyFields.iterator().next();

					for (Document doc : conflictDocs) {
						if (details.length() > 0)
							details.append(", ");
						details.append(primaryShardKey).append(": ").append(doc.get(primaryShardKey));
					}

					logger.warn("Verification found conflict: _id {} has {} documents on shard {}: {}", id,
							conflictDocs.size(), shardId, details);
					break;
				}
			}
		}

		if (conflictCount > 0) {
			logger.warn("Verification found {} remaining conflicts", conflictCount);
			return false;
		} else {
			logger.info("Verification confirms no conflicts remain");
			return true;
		}
	}

	private boolean moveChunksEfficiently(String namespace, Map<Object, Map<String, List<Document>>> conflictingIds,
			Map<Document, CountingMegachunk> docToChunkMap) {
		// Group conflicts by chunk to minimize chunk movements
		Map<CountingMegachunk, Set<Object>> chunkToIdsMap = new HashMap<>();

		// Identify all chunks involved in conflicts
		for (Map.Entry<Object, Map<String, List<Document>>> idEntry : conflictingIds.entrySet()) {
			Object id = idEntry.getKey();
			for (List<Document> docList : idEntry.getValue().values()) {
				for (Document doc : docList) {
					CountingMegachunk chunk = docToChunkMap.get(doc);
					if (chunk != null) {
						chunkToIdsMap.computeIfAbsent(chunk, k -> new HashSet<>()).add(id);
					}
				}
			}
		}

		// Prioritize chunks for movement based on conflict resolution potential
		List<ChunkMoveCandidate> candidates = new ArrayList<>();
		for (Map.Entry<CountingMegachunk, Set<Object>> entry : chunkToIdsMap.entrySet()) {
			CountingMegachunk chunk = entry.getKey();
			int impactScore = calculateImpactScore(chunk, entry.getValue(), conflictingIds);
			candidates.add(new ChunkMoveCandidate(chunk, impactScore));
		}

		// Sort by impact score (highest first)
		candidates.sort((a, b) -> Integer.compare(b.impactScore, a.impactScore));

		// Process a larger batch of chunks per iteration (3-5 instead of just 1)
		boolean madeProgress = false;
		int movedCount = 0;
		int maxMovesPerIteration = 3; // Adjust based on cluster size and stability

		for (ChunkMoveCandidate candidate : candidates) {
			if (movedCount >= maxMovesPerIteration)
				break;

			CountingMegachunk chunk = candidate.chunk;
			String sourceShard = chunk.getShard();
			String targetShard = findBestTargetShard(sourceShard);

			if (targetShard != null) {
				logger.info("Moving high-impact chunk with bounds min: {}, max: {} from shard {} to shard {}",
						chunk.getMin(), chunk.getMax(), sourceShard, targetShard);

				boolean success = moveChunkWithRetry(namespace, chunk, targetShard);
				if (success) {
					logger.info("Successfully moved high-impact chunk to shard {}", targetShard);
					chunk.setShard(targetShard);
					madeProgress = true;
					movedCount++;

					// Refresh chunk cache periodically to avoid stale data
					if (movedCount % 2 == 0) {
						refreshChunkCache(namespace);
					}
				}
			}
		}

		// Final refresh after all moves
		if (madeProgress) {
			refreshChunkCache(namespace);
		}

		return madeProgress;
	}

	/**
	 * Find the best target shard for moving a chunk
	 */
	private String findBestTargetShard(String currentShard) {
		// Simple implementation - just pick a different shard
		if (currentShard.equals("shA")) {
			return "shard_B";
		} else {
			return "shA";
		}

		// In a real implementation, would consider:
		// - Current shard load
		// - Number of chunks already moved to each shard
		// - Whether moving to this shard would create new conflicts
	}

	/**
	 * Calculate an impact score for moving a chunk based on: 1. How many conflicts
	 * it would resolve 2. How many documents it contains (smaller chunks are
	 * preferred) 3. Whether it would create new conflicts
	 * 
	 * Higher scores indicate better candidates for movement
	 */
	private int calculateImpactScore(CountingMegachunk chunk, Set<Object> affectedIds,
			Map<Object, Map<String, List<Document>>> conflictingIds) {
		int score = 0;

		// Get all documents in this chunk
		Map<Object, List<Document>> idToDocsInChunk = new HashMap<>();

		// Count how many conflicts this chunk participates in
		int conflictCount = 0;

		for (Object id : affectedIds) {
			if (conflictingIds.containsKey(id)) {
				conflictCount++;

				// Check if moving this chunk would resolve the conflict
				Map<String, List<Document>> shardToDocsMap = conflictingIds.get(id);
				String currentShard = chunk.getShard();

				// If this shard has multiple docs with this ID, moving one chunk could help
				List<Document> docsOnShard = shardToDocsMap.get(currentShard);
				if (docsOnShard != null && docsOnShard.size() > 1) {
					// Calculate how many docs with this ID are in this specific chunk
					int docsInChunk = 0;
					for (Document doc : docsOnShard) {
						if (isDocInChunk(doc, chunk)) {
							docsInChunk++;

							// Group docs by ID for later analysis
							idToDocsInChunk.computeIfAbsent(id, k -> new ArrayList<>()).add(doc);
						}
					}

					// If there's only one doc with this ID in this chunk, moving it would help
					// resolve
					if (docsInChunk == 1 && docsOnShard.size() > 1) {
						// This is ideal - moving this chunk would resolve a conflict
						score += 100;
					} else if (docsInChunk > 0) {
						// Moving would help, but not completely resolve
						score += 20;
					}
				}
			}
		}

		// Adjust for chunk size - prefer smaller chunks
		// Rough estimate based on the number of affected IDs
		int estimatedSize = idToDocsInChunk.size();
		if (estimatedSize <= 3) {
			score += 30; // Small chunk, easy to move
		} else if (estimatedSize <= 10) {
			score += 20; // Medium chunk
		} else {
			score += 10; // Large chunk, harder to move
		}

		// Adjust for chunk location
		String targetShard = chunk.getShard().equals("shA") ? "shard_B" : "shA";

		// Check if moving would create new conflicts
		boolean wouldCreateNewConflicts = false;
		for (Map.Entry<Object, List<Document>> entry : idToDocsInChunk.entrySet()) {
			Object id = entry.getKey();

			// Check if target shard already has docs with this ID
			Map<String, List<Document>> shardToDocsMap = conflictingIds.get(id);
			if (shardToDocsMap != null && shardToDocsMap.containsKey(targetShard)) {
				// Target shard already has docs with this ID
				wouldCreateNewConflicts = true;
				break;
			}
		}

		if (wouldCreateNewConflicts) {
			// Significant penalty for creating new conflicts
			score -= 80;
		} else {
			// Bonus for not creating conflicts
			score += 40;
		}

		// Adjust based on number of conflicts this chunk is involved in
		score += Math.min(conflictCount * 10, 50); // Cap at +50 to prevent too much weight

		return score;
	}

	/**
	 * Check if a document belongs to a specific chunk
	 */
	private boolean isDocInChunk(Document doc, CountingMegachunk chunk) {
		try {
			// Extract the document's shard key
			Set<String> shardKeyFields = getShardKeyFields(doc.get("_id").toString());
			if (shardKeyFields.isEmpty())
				return false;

			BsonValueWrapper docShardKey = getShardKeyWrapper(shardKeyFields, doc);

			// Get the chunk boundaries
			BsonValueWrapper minKey = new BsonValueWrapper(chunk.getMin());
			BsonValueWrapper maxKey = new BsonValueWrapper(chunk.getMax());

			// Check if the document's shard key falls within the chunk boundaries
			return docShardKey.compareTo(minKey) >= 0 && docShardKey.compareTo(maxKey) < 0;
		} catch (Exception e) {
			logger.error("Error checking if doc is in chunk: {}", e.getMessage());
			return false;
		}
	}

	/**
	 * Get the shard key fields for a namespace based on _id This is a helper that
	 * finds the namespace for a given document ID
	 */
	private Set<String> getShardKeyFields(String idString) {
		// This is a simplification - in real implementation, would need to map ID to
		// namespace
		// For this example, we'll assume we know the namespace or use a fixed set of
		// fields

		for (String namespace : collectionsMap.keySet()) {
			Document collMeta = collectionsMap.get(namespace);
			if (collMeta != null && collMeta.containsKey("key")) {
				Document shardKeyDoc = (Document) collMeta.get("key");
				return shardKeyDoc.keySet();
			}
		}

		return Collections.emptySet();
	}

	/**
	 * Improved direct clearing of conflicts with special handling for negative
	 * shard keys
	 */
	private boolean directClearShardConflicts(String namespace, String shardToClear,
			Map<Object, Map<String, List<Document>>> conflictingIds, Map<Document, CountingMegachunk> docToChunkMap) {
		logger.info("Attempting direct clearing of conflicts from shard: {}", shardToClear);

		// Get the other shard
		String targetShard = shardToClear.equals("shA") ? "shard_B" : "shA";

		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
			logger.error("No collection metadata found for namespace: {}", namespace);
			return false;
		}
		Document shardKeyDoc = (Document) collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();
		String primaryShardKey = shardKeyFields.iterator().next();

		// Special handling for documents with very negative shard key values
		Map<Object, List<Document>> negativeKeyDocs = new HashMap<>();
		Map<Object, List<Document>> positiveKeyDocs = new HashMap<>();

		// Separate documents by shard key polarity
		for (Map.Entry<Object, Map<String, List<Document>>> idEntry : conflictingIds.entrySet()) {
			Object id = idEntry.getKey();
			Map<String, List<Document>> shardMap = idEntry.getValue();
			List<Document> docsOnShard = shardMap.get(shardToClear);

			if (docsOnShard != null && docsOnShard.size() > 1) {
				boolean hasNegativeKey = false;
				boolean hasPositiveKey = false;

				for (Document doc : docsOnShard) {
					Object keyValue = doc.get(primaryShardKey);
					if (keyValue instanceof Number) {
						double numVal = ((Number) keyValue).doubleValue();
						if (numVal < -1000000000) {
							hasNegativeKey = true;
						} else {
							hasPositiveKey = true;
						}
					}
				}

				// If we have a mix of negative and positive keys, this is a good candidate for
				// splitting
				if (hasNegativeKey && hasPositiveKey) {
					logger.info("Found _id {} with mix of positive and negative shard keys", id);

					// Create separate lists for negative and positive key docs
					List<Document> negDocs = new ArrayList<>();
					List<Document> posDocs = new ArrayList<>();

					for (Document doc : docsOnShard) {
						Object keyValue = doc.get(primaryShardKey);
						if (keyValue instanceof Number) {
							double numVal = ((Number) keyValue).doubleValue();
							if (numVal < -1000000000) {
								negDocs.add(doc);
							} else {
								posDocs.add(doc);
							}
						}
					}

					if (!negDocs.isEmpty()) {
						negativeKeyDocs.put(id, negDocs);
					}
					if (!posDocs.isEmpty()) {
						positiveKeyDocs.put(id, posDocs);
					}
				}
			}
		}

		// If we have documents with mixed polarity shard keys, handle them specially
		boolean madeProgress = false;
		if (!negativeKeyDocs.isEmpty() || !positiveKeyDocs.isEmpty()) {
			logger.info("Found {} ids with negative keys and {} ids with positive keys", negativeKeyDocs.size(),
					positiveKeyDocs.size());

			// Try to move all negative key docs to one shard and positive to the other
			// First, check which shard has more negative key docs

			// Try to move chunks containing negative key documents
			for (Map.Entry<Object, List<Document>> entry : negativeKeyDocs.entrySet()) {
				Object id = entry.getKey();
				List<Document> docs = entry.getValue();

				// Group by chunk
				Map<CountingMegachunk, List<Document>> chunkToDocsMap = new HashMap<>();
				for (Document doc : docs) {
					CountingMegachunk chunk = docToChunkMap.get(doc);
					if (chunk != null) {
						chunkToDocsMap.computeIfAbsent(chunk, k -> new ArrayList<>()).add(doc);
					}
				}

				// Move each chunk to target shard
				for (CountingMegachunk chunk : chunkToDocsMap.keySet()) {
					logger.info(
							"Moving chunk with negative keys, bounds min: {}, max: {} from shard {} to shard {} for _id {}",
							chunk.getMin(), chunk.getMax(), shardToClear, targetShard, id);

					boolean success = moveChunkWithRetry(namespace, chunk, targetShard);

					if (success) {
						logger.info("Successfully moved negative key chunk to shard {}", targetShard);
						chunk.setShard(targetShard);
						madeProgress = true;
						refreshChunkCache(namespace);
						return madeProgress; // Return after first successful move to avoid race conditions
					}
				}
			}
		}

		// If we didn't make progress with negative keys, fall back to standard approach
		if (!madeProgress) {
			// Collect all chunks that have conflicts on this shard
			Set<CountingMegachunk> conflictChunks = new HashSet<>();

			for (Map.Entry<Object, Map<String, List<Document>>> idEntry : conflictingIds.entrySet()) {
				Map<String, List<Document>> shardMap = idEntry.getValue();
				List<Document> docsOnShard = shardMap.get(shardToClear);

				if (docsOnShard != null && docsOnShard.size() > 1) {
					for (Document doc : docsOnShard) {
						CountingMegachunk chunk = docToChunkMap.get(doc);
						if (chunk != null && chunk.getShard().equals(shardToClear)) {
							conflictChunks.add(chunk);
						}
					}
				}
			}

			if (conflictChunks.isEmpty()) {
				logger.info("No conflict chunks found on shard {}", shardToClear);
				return false;
			}

			logger.info("Found {} conflict chunks on shard {}", conflictChunks.size(), shardToClear);

			// Move all conflict chunks to the target shard
			for (CountingMegachunk chunk : conflictChunks) {
				logger.info("Moving conflict chunk min: {}, max: {} from shard {} to shard {}", chunk.getMin(),
						chunk.getMax(), shardToClear, targetShard);

				boolean success = moveChunkWithRetry(namespace, chunk, targetShard);

				if (success) {
					logger.info("Successfully moved conflict chunk to shard {}", targetShard);
					chunk.setShard(targetShard);
					madeProgress = true;

					// Refresh chunk cache after each move to ensure we have an up-to-date view
					refreshChunkCache(namespace);
					break; // One move at a time to avoid creating new conflicts
				}
			}
		}

		return madeProgress;
	}

	/**
	 * Force resolution for stubborn conflicts - more aggressive approach when
	 * normal methods fail
	 */
	private boolean forceResolveConflict(String namespace, Object id, Map<String, List<Document>> shardToDocsMap,
			Map<Document, CountingMegachunk> docToChunkMap) {
		logger.info("Force resolving conflict for _id: {}", id);

		// Get collection metadata for shard key information
		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
			logger.error("No collection metadata found for namespace: {}", namespace);
			return false;
		}
		Document shardKeyDoc = (Document) collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();
		String primaryShardKey = shardKeyFields.iterator().next();

		boolean madeProgress = false;

		// Process each shard that has conflicts for this ID
		for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
			String shard = shardEntry.getKey();
			List<Document> docsOnShard = shardEntry.getValue();

			if (docsOnShard.size() <= 1)
				continue;

			String targetShard = shard.equals("shA") ? "shard_B" : "shA";

			logger.info("Forcing resolution for _id {} with {} documents on shard {}", id, docsOnShard.size(), shard);

			// First, try to split the shard key range
			// Sort documents by shard key value
			docsOnShard.sort((a, b) -> {
				Object valA = a.get(primaryShardKey);
				Object valB = b.get(primaryShardKey);

				if (valA == null || valB == null)
					return 0;

				if (valA instanceof Number && valB instanceof Number) {
					return Double.compare(((Number) valA).doubleValue(), ((Number) valB).doubleValue());
				}

				return valA.toString().compareTo(valB.toString());
			});

			// Create explicit splits between documents if possible
			for (int i = 0; i < docsOnShard.size() - 1; i++) {
				Document doc1 = docsOnShard.get(i);
				Document doc2 = docsOnShard.get(i + 1);

				Object val1 = doc1.get(primaryShardKey);
				Object val2 = doc2.get(primaryShardKey);

				// Only try to split if the values are different
				if (val1 != null && val2 != null && !val1.equals(val2)) {
					if (val1 instanceof Number && val2 instanceof Number) {
						double num1 = ((Number) val1).doubleValue();
						double num2 = ((Number) val2).doubleValue();

						// Only try to split if there's enough space between values
						if (Math.abs(num2 - num1) > 1.0) {
							double splitPoint = num1 + (num2 - num1) / 2.0;

							// Create a find document for the split
							Document findDoc = new Document(primaryShardKey, splitPoint);
							logger.info("Creating explicit split point at {} between {} and {}", splitPoint, num1,
									num2);

							// Convert to BsonDocument
							BsonDocument bsonFindDoc = BsonValueConverter.convertToBsonDocument(findDoc);

							// Attempt the split
							Document result = destShardClient.splitFind(namespace, bsonFindDoc, true);

							if (result != null) {
								logger.info("Successfully created explicit split at {}", splitPoint);
								madeProgress = true;
								refreshChunkCache(namespace);
							}
						}
					}
				}
			}

			// Even if we were able to create splits, try moving chunks
			// Create a fresh mapping of documents to chunks after splitting
			Map<Document, CountingMegachunk> freshDocToChunkMap = new HashMap<>();
			for (Document doc : docsOnShard) {
				try {
					BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
					NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);

					if (chunkMap != null) {
						Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap
								.floorEntry(shardKeyWrapper);
						if (chunkEntry != null) {
							freshDocToChunkMap.put(doc, chunkEntry.getValue());
						}
					}
				} catch (Exception e) {
					logger.error("Error updating chunk for doc with _id {}: {}", doc.get("_id"), e.getMessage());
				}
			}

			// Group documents by chunk
			Map<CountingMegachunk, List<Document>> chunkToDocsMap = new HashMap<>();
			for (Document doc : docsOnShard) {
				CountingMegachunk chunk = freshDocToChunkMap.get(doc);
				if (chunk != null) {
					chunkToDocsMap.computeIfAbsent(chunk, k -> new ArrayList<>()).add(doc);
				}
			}

			// Try to move chunks with only one document with this ID first
			List<CountingMegachunk> singleDocChunks = new ArrayList<>();
			List<CountingMegachunk> multiDocChunks = new ArrayList<>();

			for (Map.Entry<CountingMegachunk, List<Document>> entry : chunkToDocsMap.entrySet()) {
				if (entry.getValue().size() == 1) {
					singleDocChunks.add(entry.getKey());
				} else {
					multiDocChunks.add(entry.getKey());
				}
			}

			// Prioritize single doc chunks for movement
			List<CountingMegachunk> allChunks = new ArrayList<>();
			allChunks.addAll(singleDocChunks);
			allChunks.addAll(multiDocChunks);

			// Try to move each chunk to the other shard
			for (CountingMegachunk chunk : allChunks) {
				logger.info("Force-moving chunk with bounds min: {}, max: {} from shard {} to shard {} for _id {}",
						chunk.getMin(), chunk.getMax(), shard, targetShard, id);

				boolean success = moveChunkWithRetry(namespace, chunk, targetShard);

				if (success) {
					logger.info("Successfully force-moved chunk to shard {}", targetShard);
					chunk.setShard(targetShard);
					madeProgress = true;
					refreshChunkCache(namespace);

					// After a successful move, check if conflict is resolved
					if (verifyConflictResolved(namespace, id, freshDocToChunkMap)) {
						logger.info("Conflict resolution verified for _id {}", id);
						return true;
					}

					// If we moved a single-doc chunk, we can stop - this should have helped
					if (singleDocChunks.contains(chunk)) {
						break;
					}
				}
			}

			// If we're still stuck with conflicts, try more aggressive splitting
			if (!madeProgress && docsOnShard.size() >= 2) {
				// For now, just try to move any remaining chunks
				for (Document doc : docsOnShard) {
					CountingMegachunk chunk = freshDocToChunkMap.get(doc);
					if (chunk != null && chunk.getShard().equals(shard)) {
						logger.info("Last resort: moving chunk for doc with shard key {} to {} for _id {}",
								doc.get(primaryShardKey), targetShard, id);

						boolean success = moveChunkWithRetry(namespace, chunk, targetShard);

						if (success) {
							logger.info("Successfully moved last resort chunk to shard {}", targetShard);
							chunk.setShard(targetShard);
							madeProgress = true;
							refreshChunkCache(namespace);
							break;
						}
					}
				}
			}
		}

		return madeProgress;
	}

	/**
	 * Verify if a specific conflict has been resolved
	 */
	private boolean verifyConflictResolved(String namespace, Object id,
			Map<Document, CountingMegachunk> docToChunkMap) {
		Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
		List<Document> docsWithId = duplicatesMap.get(id);

		if (docsWithId == null || docsWithId.size() <= 1) {
			return true; // Not a duplicate or already removed
		}

		// Group by shard
		Map<String, List<Document>> shardToDocsMap = new HashMap<>();

		for (Document doc : docsWithId) {
			CountingMegachunk chunk = docToChunkMap.get(doc);
			if (chunk == null)
				continue;

			String shard = chunk.getShard();
			shardToDocsMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(doc);
		}

		// Check if any shard has multiple documents with this ID
		for (Map.Entry<String, List<Document>> entry : shardToDocsMap.entrySet()) {
			if (entry.getValue().size() > 1) {
				return false; // Still have a conflict
			}
		}

		return true; // No conflicts found
	}

	/**
	 * Improved implementation of executeSplitsAndMigrations with better conflict
	 * targeting and efficiency
	 */
	public void executeSplitsAndMigrations() {
		// First execute any pre-calculated splits
		executePreCalculatedSplits();

		logger.info("Starting optimized conflict resolution process");

		// Process each namespace with duplicates
		for (String namespace : duplicateIdToDocsMap.keySet()) {
			logger.info("Processing namespace: {}", namespace);

			// Get shard key information
			Document collMeta = collectionsMap.get(namespace);
			if (collMeta == null) {
				logger.error("No collection metadata found for namespace: {}", namespace);
				continue;
			}

			Document shardKeyDoc = (Document) collMeta.get("key");
			Set<String> shardKeyFields = shardKeyDoc.keySet();
			String primaryShardKey = shardKeyFields.iterator().next();

			// Set of _ids that have been successfully resolved
			Set<Object> resolvedIds = new HashSet<>();

			// For tracking persistent conflicts across iterations
			Set<Object> persistentConflicts = new HashSet<>();
			Set<Object> previouslyUnresolvedIds = new HashSet<>();
			boolean madeSomeProgress = false;

			// Maximum number of iterations to try
			final int MAX_ITERATIONS = 25; // Reduced from 20 since we should be more efficient

			// Run multiple iterations of conflict resolution
			for (int iteration = 1; iteration <= MAX_ITERATIONS; iteration++) {
				logger.info("Starting iteration {} of conflict resolution", iteration);

				// Map of document to its chunk
				Map<Document, CountingMegachunk> docToChunkMap = new HashMap<>();
				Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);

				// For each duplicate _id
				for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
					Object id = entry.getKey();
					List<Document> docsWithSameId = entry.getValue();

					// Skip if not a duplicate or already resolved
					if (docsWithSameId.size() <= 1 || resolvedIds.contains(id))
						continue;

					// For each document with this _id
					for (Document doc : docsWithSameId) {
						// Find which chunk this document belongs to
						try {
							BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
							NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
							if (chunkMap == null)
								continue;

							Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap
									.floorEntry(shardKeyWrapper);
							if (chunkEntry == null)
								continue;

							CountingMegachunk chunk = chunkEntry.getValue();
							docToChunkMap.put(doc, chunk);
						} catch (Exception e) {
							logger.error("Error finding chunk for doc with _id {}: {}", id, e.getMessage());
						}
					}
				}

				// Analyze which _ids are in conflict (on same shard)
				Map<Object, Map<String, List<Document>>> conflictingIds = new HashMap<>();
				Map<String, Integer> conflictsByShardCount = new HashMap<>();

				for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
					Object id = entry.getKey();
					List<Document> docsWithSameId = entry.getValue();

					// Skip if not a duplicate or already resolved
					if (docsWithSameId.size() <= 1 || resolvedIds.contains(id))
						continue;

					// Track if this ID has a conflict on any shard
					boolean hasConflictOnAnyShard = false;

					// Group documents by shard
					Map<String, List<Document>> shardToDocsMap = new HashMap<>();

					for (Document doc : docsWithSameId) {
						CountingMegachunk chunk = docToChunkMap.get(doc);
						if (chunk == null)
							continue;

						String shardId = chunk.getShard();
						shardToDocsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(doc);
					}

					// If multiple docs are on the same shard, we have a conflict
					for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
						if (shardEntry.getValue().size() > 1) {
							hasConflictOnAnyShard = true;

							if (conflictingIds.containsKey(id)) {
								conflictingIds.get(id).put(shardEntry.getKey(), shardEntry.getValue());
							} else {
								Map<String, List<Document>> newMap = new HashMap<>();
								newMap.put(shardEntry.getKey(), shardEntry.getValue());
								conflictingIds.put(id, newMap);
							}

							// Count conflicts by shard
							String shard = shardEntry.getKey();
							conflictsByShardCount.put(shard, conflictsByShardCount.getOrDefault(shard, 0) + 1);

							// Log the conflict
							logger.info("Conflict in iteration {}: _id {} has {} documents on shard {}", iteration, id,
									shardEntry.getValue().size(), shard);

							// Log the documents
							for (Document doc : shardEntry.getValue()) {
								CountingMegachunk chunk = docToChunkMap.get(doc);
								Object shardKeyValue = doc.get(primaryShardKey);

								logger.info("  Doc with _id {}, shardKey: {}, in chunk with bounds: min={}, max={}", id,
										shardKeyValue, chunk.getMin(), chunk.getMax());
							}
						}
					}

					// Check if document distribution constitutes a proper resolution
					// Only mark as resolved if documents are distributed across multiple shards
					// and no shard has more than one document with this ID
					if (!hasConflictOnAnyShard && shardToDocsMap.size() > 1) {
						resolvedIds.add(id);
						logger.info("Resolved in iteration {}: _id {} is on shards: {}", iteration, id,
								shardToDocsMap.keySet());
					} else if (hasConflictOnAnyShard) {
						// If this ID was previously marked as resolved but has conflicts again,
						// remove it from the resolved set
						if (resolvedIds.contains(id)) {
							resolvedIds.remove(id);
							logger.warn("Previously resolved _id {} has conflicts again", id);
						}
					}
				}

				// Track currently unresolved IDs for progress monitoring
				Set<Object> currentUnresolvedIds = new HashSet<>(conflictingIds.keySet());

				// Check if we made progress in this iteration
				boolean iterationMadeProgress = false;
				if (!previouslyUnresolvedIds.isEmpty()) {
					// Check if any previously unresolved IDs are now resolved
					Set<Object> nowResolved = new HashSet<>(previouslyUnresolvedIds);
					nowResolved.removeAll(currentUnresolvedIds);

					if (!nowResolved.isEmpty()) {
						logger.info("Made progress: resolved {} previously unresolved IDs in iteration {}",
								nowResolved.size(), iteration);
						iterationMadeProgress = true;
						madeSomeProgress = true;
					}
				}

				// Update tracking for persistent conflicts
				if (iteration > 1) {
					if (persistentConflicts.isEmpty()) {
						persistentConflicts.addAll(currentUnresolvedIds);
					} else {
						// Keep only conflicts that persisted from last iteration
						persistentConflicts.retainAll(currentUnresolvedIds);
					}
				}

				// Update previously unresolved IDs for next iteration
				previouslyUnresolvedIds = new HashSet<>(currentUnresolvedIds);

				// If no conflicts left, we're done
				if (conflictingIds.isEmpty()) {
					logger.info("No conflicts remaining after iteration {}, breaking early", iteration);

					// Verify we didn't miss any conflicts before breaking
					boolean actuallyResolved = verifyNoConflictsRemain(namespace, docToChunkMap, duplicatesMap);
					if (actuallyResolved) {
						break;
					}
				}

				logger.info("Found {} conflicting _ids in iteration {}", conflictingIds.size(), iteration);
				
	            // Check for cycles and try to break them with splitFind
	            if (iteration > 3) {
	                // Check if any conflicts persist for too many iterations
	                for (Object id : conflictingIds.keySet()) {
	                    if (persistentConflicts.contains(id)) {
	                        // If this ID is in a cycle
	                        if (!cycleDetectedIds.contains(id)) {
	                            cycleDetectedIds.add(id);
	                            logger.info("Cycle detected for _id: {}", id);
	                            
	                            // Break the cycle with splitFind
	                            boolean cycleHandled = breakCycleWithSplitFind(namespace, id, 
	                                                                      conflictingIds.get(id), 
	                                                                      docToChunkMap);
	                            
	                            if (cycleHandled) {
	                                logger.info("Successfully broke cycle for _id: {}", id);
	                                refreshChunkCache(namespace);
	                                iterationMadeProgress = true;
	                                madeSomeProgress = true;
	                                break; // Handle one cycle at a time
	                            }
	                        }
	                    }
	                }
	            }
	            
	            // More aggressive cycle breaking for very persistent conflicts
	            if (iteration > 10 && !iterationMadeProgress && !persistentConflicts.isEmpty()) {
	                // Try more aggressive handling - directly use splitFind on all persistent conflicts
	                logger.info("Attempting aggressive resolution for {} persistent conflicts", 
	                           persistentConflicts.size());
	                
	                for (Object id : persistentConflicts) {
	                    if (conflictingIds.containsKey(id)) {
	                        boolean handled = breakCycleWithSplitFind(namespace, id, 
	                                                              conflictingIds.get(id), 
	                                                              docToChunkMap);
	                        
	                        if (handled) {
	                            logger.info("Successfully addressed persistent conflict for _id: {}", id);
	                            refreshChunkCache(namespace);
	                            iterationMadeProgress = true;
	                            madeSomeProgress = true;
	                            break; // Handle one at a time
	                        }
	                    }
	                }
	            }

				// Multi-stage resolution approach
				int resolutionsBatch = Math.min(3, conflictingIds.size() / 3); // Handle more conflicts per iteration
				int resolvedCount = 0;
				boolean madeProgress = false;

				// 1. Try to resolve the most impactful conflicts using efficient chunk
				// movements
				// This can resolve multiple conflicts at once by targeting high-value chunks
				madeProgress = moveChunksEfficiently(namespace, conflictingIds, docToChunkMap);
				if (madeProgress) {
					refreshChunkCache(namespace);
					continue;
				}

				// 2. If efficient movements didn't help, try targeting specific conflicts on
				// the worst shard
				String shardToClear = getShardWithMostConflicts(conflictsByShardCount);
				if (shardToClear != null) {
					madeProgress = directClearShardConflicts(namespace, shardToClear, conflictingIds, docToChunkMap);

					if (madeProgress) {
						refreshChunkCache(namespace);
						continue;
					}
				}

				// 3. If direct clearing didn't help, try explicit splitting for high-value
				// candidates
				List<Object> splitCandidates = findBestSplitCandidates(namespace, conflictingIds, docToChunkMap,
						primaryShardKey);

				// Try more splits per iteration - don't break after first success
				int splitsToAttempt = Math.min(resolutionsBatch, splitCandidates.size());
				for (int i = 0; i < splitsToAttempt; i++) {
					Object id = splitCandidates.get(i);
					if (explicitSplitForKey(namespace, id, docToChunkMap, primaryShardKey)) {
						resolvedCount++;
						madeProgress = true;

						// Refresh less frequently - only after multiple splits
						if (resolvedCount % 2 == 0) {
							refreshChunkCache(namespace);
						}
					}
				}

				// Final refresh if we did any splits but didn't refresh yet
				if (madeProgress && resolvedCount % 2 != 0) {
					refreshChunkCache(namespace);
					continue;
				}

				// 4. If we're still stuck, use more aggressive conflict resolution
				// Start this earlier when we detect lack of progress
				boolean useAggressive = (iteration >= 3 && !persistentConflicts.isEmpty())
						|| (iteration >= 2 && !iterationMadeProgress && !madeSomeProgress);

				if (useAggressive) {
					logger.warn("Using aggressive resolution strategies in iteration {}", iteration);

					// Target the most persistent conflicts first
					Set<Object> targets = persistentConflicts.isEmpty() ? new HashSet<>(conflictingIds.keySet())
							: persistentConflicts;

					// Try to resolve multiple persistent conflicts in one iteration
					resolvedCount = 0;
					for (Object id : targets) {
						if (resolvedCount >= resolutionsBatch)
							break;

						if (conflictingIds.containsKey(id)) {
							boolean progress = forceResolveConflict(namespace, id, conflictingIds.get(id),
									docToChunkMap);
							if (progress) {
								resolvedCount++;
								madeProgress = true;

								// Refresh cache periodically
								if (resolvedCount % 2 == 0) {
									refreshChunkCache(namespace);
								}
							}
						}
					}

					// Final refresh if needed
					if (madeProgress && resolvedCount % 2 != 0) {
						refreshChunkCache(namespace);
					}
				}

				// If we've made no progress at all in this iteration, log a warning
				if (!madeProgress) {
					logger.warn("No progress made in iteration {}. {} conflicts remain unresolved.", iteration,
							conflictingIds.size());

					// If we've gone several iterations with no progress and we're past iteration 5,
					// try a more desperate approach or consider breaking early
					if (iteration > 5 && !iterationMadeProgress && !madeSomeProgress) {
						logger.warn(
								"Multiple iterations with no progress. Trying last resort measures or considering early termination.");

						// Last resort: Try to move any chunks that might help
						boolean lastResortProgress = false;

						// If we have a small number of remaining conflicts, try to force-resolve them
						// all
						if (conflictingIds.size() <= 10) {
							for (Object id : conflictingIds.keySet()) {
								if (forceResolveConflict(namespace, id, conflictingIds.get(id), docToChunkMap)) {
									lastResortProgress = true;
									refreshChunkCache(namespace);
									break;
								}
							}
						}

						// If that didn't work, we'll continue with the next iteration, but we might
						// not be able to make further progress with current strategies
						if (!lastResortProgress && iteration > 10) {
							logger.warn(
									"Unable to make progress after multiple iterations. Some conflicts may require manual intervention.");
							// We could break here, but let's complete all iterations in case later ones
							// help
						}
					}
				}
			}

			// Final verification
			verifyDuplicateResolution(namespace, shardKeyFields);
		}
	}

	private boolean breakCycleWithSplitFind(String namespace, Object id, Map<String, List<Document>> shardToDocsMap,
			Map<Document, CountingMegachunk> docToChunkMap) {

		logger.info("Breaking cycle for _id: {} using splitFind", id);

		// Get collection metadata for shard key information
		Document collMeta = collectionsMap.get(namespace);
		if (collMeta == null) {
			logger.error("No collection metadata found for namespace: {}", namespace);
			return false;
		}
		Document shardKeyDoc = (Document) collMeta.get("key");
		Set<String> shardKeyFields = shardKeyDoc.keySet();
		String primaryShardKey = shardKeyFields.iterator().next();

		boolean madeProgress = false;

		// For each shard that has conflicts
		for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
			String shard = shardEntry.getKey();
			List<Document> docsOnShard = shardEntry.getValue();

			if (docsOnShard.size() <= 1)
				continue;

			// Get the shard key values of the documents
			for (Document doc : docsOnShard) {
				Object shardKeyValue = doc.get(primaryShardKey);

				// Use the exact shard key value for the split
				Document splitDoc = new Document(primaryShardKey, shardKeyValue);
				logger.info("Using splitFind directly on shard key value: {} for _id: {}", shardKeyValue, id);

				// Convert to BsonDocument
				BsonDocument bsonSplitDoc = BsonValueConverter.convertToBsonDocument(splitDoc);

				// Attempt the split - using exact shard key as split point
				Document result = destShardClient.splitFind(namespace, bsonSplitDoc, true);

				if (result != null) {
					logger.info("Successfully executed splitFind at {}", shardKeyValue);
					madeProgress = true;
				}
			}
		}

		// If we created any splits, refresh cache and move chunks
		if (madeProgress) {
			refreshChunkCache(namespace);

			// Get fresh chunk mappings
			Map<Document, CountingMegachunk> freshDocToChunkMap = new HashMap<>();
			Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
			List<Document> docsWithId = duplicatesMap.get(id);

			// Map documents to chunks
			for (Document doc : docsWithId) {
				BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
				NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);

				if (chunkMap != null) {
					Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyWrapper);
					if (chunkEntry != null) {
						freshDocToChunkMap.put(doc, chunkEntry.getValue());
					}
				}
			}

			// Try to move chunks to different shards
			for (int i = 0; i < docsWithId.size(); i++) {
				Document doc = docsWithId.get(i);
				CountingMegachunk chunk = freshDocToChunkMap.get(doc);

				if (chunk != null) {
					String currentShard = chunk.getShard();
					String targetShard = (i % 2 == 0) ? "shA" : "shard_B";

					// Only move if not already on target shard
					if (!targetShard.equals(currentShard)) {
						logger.info("Moving chunk with _id {} from shard {} to {} after splitFind", id, currentShard,
								targetShard);

						boolean moved = moveChunkWithRetry(namespace, chunk, targetShard);
						if (moved) {
							logger.info("Successfully moved chunk to shard {}", targetShard);
							break; // One move at a time to avoid complications
						}
					}
				}
			}

			// Refresh again after moves
			refreshChunkCache(namespace);
		}

		return madeProgress;
	}

	/**
	 * Determines which shard has the most conflicts to prioritize resolution
	 * 
	 * @param conflictsByShardCount Map tracking number of conflicts per shard
	 * @return The shard with the most conflicts, or null if none have conflicts
	 */
	private String getShardWithMostConflicts(Map<String, Integer> conflictsByShardCount) {
		if (conflictsByShardCount.isEmpty()) {
			return null;
		}

		String shardWithMostConflicts = null;
		int maxConflicts = 0;

		for (Map.Entry<String, Integer> entry : conflictsByShardCount.entrySet()) {
			if (entry.getValue() > maxConflicts) {
				maxConflicts = entry.getValue();
				shardWithMostConflicts = entry.getKey();
			}
		}

		// Log the decision for transparency
		if (shardWithMostConflicts != null) {
			logger.info("Targeting shard {} with {} conflicts for resolution", shardWithMostConflicts, maxConflicts);
		}

		return shardWithMostConflicts;
	}

	/**
	 * Execute all pre-calculated split points
	 */
	private void executePreCalculatedSplits() {
		logger.info("Executing pre-calculated split points");

		int splitCount = 0;
		int successCount = 0;

		// For each shard
		for (Map.Entry<String, List<ChunkSplitInfo>> entry : shardToSplitInfoMap.entrySet()) {
			String shardId = entry.getKey();
			List<ChunkSplitInfo> splitInfos = entry.getValue();

			logger.info("Executing {} split points for shard {}", splitInfos.size(), shardId);

			// For each chunk that needs splitting on this shard
			for (ChunkSplitInfo splitInfo : splitInfos) {
				String namespace = splitInfo.getNamespace();
				CountingMegachunk chunk = splitInfo.getChunk();

				// Get the collection metadata to find shard key
				Document collMeta = collectionsMap.get(namespace);
				if (collMeta == null)
					continue;

				Document shardKeyDoc = (Document) collMeta.get("key");
				String primaryShardKey = shardKeyDoc.keySet().iterator().next();

				// Execute splits for this chunk
				for (Document splitPoint : calculateSplitPoints(namespace, shardKeyDoc.keySet(),
						getShardKeyValuesForDocs(splitInfo.getSplitDocs(), primaryShardKey))) {
					splitCount++;

					// Convert to BsonDocument
					BsonDocument bsonSplitPoint = BsonValueConverter.convertToBsonDocument(splitPoint);

					// Execute the split
					logger.info("Executing split at {} for chunk with bounds: min={}, max={} on shard {}", splitPoint,
							chunk.getMin(), chunk.getMax(), shardId);

					Document result = destShardClient.splitFind(namespace, bsonSplitPoint, true);

					if (result != null) {
						successCount++;
						logger.info("Successfully executed split at {}", splitPoint);
					}
				}

				// Refresh chunk cache after splits
				if (successCount > 0) {
					refreshChunkCache(namespace);
				}
			}
		}

		logger.info("Pre-calculated split execution complete. Attempted: {}, Succeeded: {}", splitCount, successCount);
	}

	// Helper to get shard key values from documents
	private List<BsonValueWrapper> getShardKeyValuesForDocs(List<Document> docs, String primaryShardKey) {
		List<BsonValueWrapper> values = new ArrayList<>();
		for (Document doc : docs) {
			Object keyValue = doc.get(primaryShardKey);
			if (keyValue != null) {
				BsonValue bsonValue = BsonValueConverter.convertToBsonValue(keyValue);
				values.add(new BsonValueWrapper(bsonValue));
			}
		}
		Collections.sort(values);
		return values;
	}

	/**
	 * Find best candidates for splitting
	 */
	private List<Object> findBestSplitCandidates(String namespace,
			Map<Object, Map<String, List<Document>>> conflictingIds, Map<Document, CountingMegachunk> docToChunkMap,
			String primaryShardKey) {
		// Return all IDs sorted by shard key difference
		List<Object> candidates = new ArrayList<>(conflictingIds.keySet());

		candidates.sort((id1, id2) -> {
			// Calculate max shard key difference for each ID
			double diff1 = calculateKeyDifference(id1, conflictingIds.get(id1), primaryShardKey);
			double diff2 = calculateKeyDifference(id2, conflictingIds.get(id2), primaryShardKey);

			// Sort by largest difference first (better candidates for splitting)
			return Double.compare(diff2, diff1);
		});

		return candidates;
	}

	/**
	 * Calculate maximum difference between shard key values for documents with the
	 * same ID
	 */
	private double calculateKeyDifference(Object id, Map<String, List<Document>> shardToDocsMap,
			String primaryShardKey) {
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;

		for (List<Document> docs : shardToDocsMap.values()) {
			for (Document doc : docs) {
				Object value = doc.get(primaryShardKey);
				if (value instanceof Number) {
					double numValue = ((Number) value).doubleValue();
					min = Math.min(min, numValue);
					max = Math.max(max, numValue);
				}
			}
		}

		return max - min;
	}

	/**
	 * Implement a function to explicitly split chunks between two key ranges with
	 * the same _id
	 */
	private boolean explicitSplitForKey(String namespace, Object id, Map<Document, CountingMegachunk> docToChunkMap,
			String primaryShardKey) {
		logger.info("Attempting explicit split for _id {}", id);

		// Get all documents with this ID
		Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
		List<Document> docsWithId = duplicatesMap.get(id);

		if (docsWithId == null || docsWithId.size() <= 1)
			return false;

		// Group documents by shard
		Map<String, List<Document>> shardToDocsMap = new HashMap<>();
		for (Document doc : docsWithId) {
			CountingMegachunk chunk = docToChunkMap.get(doc);
			if (chunk == null)
				continue;

			String shardId = chunk.getShard();
			shardToDocsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(doc);
		}

		boolean anySuccess = false;

		// For each shard with multiple docs with the same ID
		for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
			if (shardEntry.getValue().size() <= 1)
				continue;

			String shard = shardEntry.getKey();
			List<Document> docsOnShard = shardEntry.getValue();

			// Sort documents by shard key
			docsOnShard.sort((a, b) -> {
				Object valA = a.get(primaryShardKey);
				Object valB = b.get(primaryShardKey);

				if (valA == null || valB == null)
					return 0;

				if (valA instanceof Number && valB instanceof Number) {
					return Double.compare(((Number) valA).doubleValue(), ((Number) valB).doubleValue());
				}

				return valA.toString().compareTo(valB.toString());
			});

			// For each pair of documents, try to create split points in between
			for (int i = 0; i < docsOnShard.size() - 1; i++) {
				Document doc1 = docsOnShard.get(i);
				Document doc2 = docsOnShard.get(i + 1);

				CountingMegachunk chunk1 = docToChunkMap.get(doc1);
				CountingMegachunk chunk2 = docToChunkMap.get(doc2);

				// Skip if they're in the same chunk (would be handled by regular split)
				if (chunk1.equals(chunk2))
					continue;

				// Check if they're in very different ranges (likely need multiple splits)
				Object val1 = doc1.get(primaryShardKey);
				Object val2 = doc2.get(primaryShardKey);

				if (val1 instanceof Number && val2 instanceof Number) {
					double num1 = ((Number) val1).doubleValue();
					double num2 = ((Number) val2).doubleValue();

					// If the values are very far apart, we need to create intermediate split points
					double distance = Math.abs(num2 - num1);
					int numSplits = (int) Math.min(5, Math.max(1, Math.floor(distance / 10000000)));

					boolean splitsCreated = false;

					for (int split = 1; split <= numSplits; split++) {
						double splitPoint = num1 + (num2 - num1) * split / (numSplits + 1);

						// Create a find document for the split
						Document findDoc = new Document(primaryShardKey, splitPoint);
						logger.info("Creating explicit split point at {} between {} and {}", splitPoint, num1, num2);

						// Convert to BsonDocument
						BsonDocument bsonFindDoc = BsonValueConverter.convertToBsonDocument(findDoc);

						// Attempt the split
						Document result = destShardClient.splitFind(namespace, bsonFindDoc, false);

						if (result != null) {
							logger.info("Successfully created explicit split at {}", splitPoint);
							splitsCreated = true;
							anySuccess = true;
						}
					}

					// Important: Refresh the chunk cache after creating splits
					if (splitsCreated) {
						refreshChunkCache(namespace);

						// Since we've refreshed the cache, we need to update our local chunk map too
						// This will prevent errors when trying to move chunks with old boundaries
						NavigableMap<BsonValueWrapper, CountingMegachunk> freshChunkMap = destChunkMap.get(namespace);

						if (freshChunkMap != null) {
							// Re-map the docs to their possibly new chunks
							for (Document doc : docsWithId) {
								Object shardKeyValue = doc.get(primaryShardKey);
								if (shardKeyValue != null) {
									try {
										BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(
												Collections.singleton(primaryShardKey), doc);
										Map.Entry<BsonValueWrapper, CountingMegachunk> entry = freshChunkMap
												.floorEntry(shardKeyWrapper);
										if (entry != null) {
											docToChunkMap.put(doc, entry.getValue());
										}
									} catch (Exception e) {
										logger.error("Error remapping doc to chunk after split: {}", e.getMessage());
									}
								}
							}

							// After refreshing, get the updated chunks
							chunk1 = docToChunkMap.get(doc1);
							chunk2 = docToChunkMap.get(doc2);
						}
					}

					// After creating splits, try to move one of the chunks
					if (splitsCreated) {
						Set<String> allShards = new HashSet<>();
						allShards.add("shA");
						allShards.add("shard_B");

						String targetShard = null;
						for (String s : allShards) {
							if (!s.equals(shard)) {
								targetShard = s;
								break;
							}
						}

						if (targetShard != null && chunk1 != null) {
							logger.info(
									"Attempting to move chunk for doc1 to {} after explicit split, bounds: min={}, max={}",
									targetShard, chunk1.getMin(), chunk1.getMax());

							boolean success = moveChunkWithRetry(namespace, chunk1, targetShard);
							if (success) {
								logger.info("Successfully moved chunk for doc1 to {} after explicit split",
										targetShard);
								anySuccess = true;

								// Refresh cache again after move
								refreshChunkCache(namespace);
								break;
							} else if (chunk2 != null) {
								logger.info(
										"Attempting to move chunk for doc2 to {} after explicit split, bounds: min={}, max={}",
										targetShard, chunk2.getMin(), chunk2.getMax());

								success = moveChunkWithRetry(namespace, chunk2, targetShard);
								if (success) {
									logger.info("Successfully moved chunk for doc2 to {} after explicit split",
											targetShard);
									anySuccess = true;

									// Refresh cache after move
									refreshChunkCache(namespace);
									break;
								}
							}
						}
					}
				}
			}
		}

		return anySuccess;
	}

	/**
	 * Helper class to bundle chunk selection information with impact score
	 */
	private static class ChunkMoveCandidate {
		final CountingMegachunk chunk;
		final int impactScore;

		ChunkMoveCandidate(CountingMegachunk chunk, int impactScore) {
			this.chunk = chunk;
			this.impactScore = impactScore;
		}
	}

	private void verifyDuplicateResolution(String namespace, Set<String> shardKeyFields) {
		String primaryShardKey = shardKeyFields.iterator().next();
		Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
		int remainingConflicts = 0;

		logger.info("Verifying resolution for {} duplicate _ids in namespace {}", duplicatesMap.size(), namespace);

		for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
			Object id = entry.getKey();
			List<Document> docsWithSameId = entry.getValue();

			// Skip if not a duplicate
			if (docsWithSameId.size() <= 1)
				continue;

			// Find which shard each document is on
			Map<String, List<Document>> shardToDocsMap = new HashMap<>();

			for (Document doc : docsWithSameId) {
				BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);

				NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
				if (chunkMap == null)
					continue;

				Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyValue);
				if (chunkEntry == null)
					continue;

				CountingMegachunk chunk = chunkEntry.getValue();

				// Add document to list for this shard
				shardToDocsMap.computeIfAbsent(chunk.getShard(), k -> new ArrayList<>()).add(doc);
			}

			// Check if duplicates are on the same shard
			if (shardToDocsMap.size() == 1) {
				String shard = shardToDocsMap.keySet().iterator().next();
				List<Document> docsOnShard = shardToDocsMap.get(shard);

				if (docsOnShard.size() > 1) {
					StringBuilder details = new StringBuilder();
					for (Document doc : docsOnShard) {
						if (details.length() > 0)
							details.append(", ");
						details.append(primaryShardKey).append(": ").append(doc.get(primaryShardKey));
					}

					logger.warn("REMAINING CONFLICT: _id {} has {} documents on shard {}: {}", id, docsOnShard.size(),
							shard, details);

					remainingConflicts++;
				}
			} else {
				// Log successful resolution
				logger.info("Successfully resolved conflict for _id {}, documents are on shards: {}", id,
						shardToDocsMap.keySet());
			}
		}

		if (remainingConflicts > 0) {
			logger.warn("Found {} remaining _id conflicts after migrations for namespace {}", remainingConflicts,
					namespace);
		} else {
			logger.info("Successfully resolved all conflicts for namespace {}", namespace);
		}
	}

	/**
	 * Moves a chunk to a different shard with retry logic to handle transient
	 * failures
	 * 
	 * @param namespace     The namespace the chunk belongs to
	 * @param chunk         The chunk to move
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
				success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), targetShardId, false,
						false, true, false, false);

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
			logger.debug("Not enough shard key values to create split points for namespace: {}", namespace);
			return splitPoints;
		}

		// Log the range of values we're working with for debugging
		logger.debug("Calculating split points for namespace: {} with shard key values range: min={}, max={}",
				namespace, sortedShardKeyValues.get(0), sortedShardKeyValues.get(sortedShardKeyValues.size() - 1));

		// We'll create splits at regular intervals
		int splitInterval = Math.max(1, sortedShardKeyValues.size() / 3); // Don't create too many splits
		logger.debug("Using split interval of {} for {} shard key values", splitInterval, sortedShardKeyValues.size());

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
					logger.debug("Generated split point at index {}: key={}, value={} (int32)", i, keyField, keyValue);
				} else if (bsonValue.isInt64()) {
					keyValue = bsonValue.asInt64().getValue();
					logger.debug("Generated split point at index {}: key={}, value={} (int64)", i, keyField, keyValue);
				} else if (bsonValue.isDouble()) {
					keyValue = bsonValue.asDouble().getValue();
					logger.debug("Generated split point at index {}: key={}, value={} (double)", i, keyField, keyValue);
				} else if (bsonValue.isString()) {
					keyValue = bsonValue.asString().getValue();
					logger.debug("Generated split point at index {}: key={}, value={} (string)", i, keyField, keyValue);
				} else if (bsonValue.isObjectId()) {
					keyValue = bsonValue.asObjectId().getValue();
					logger.debug("Generated split point at index {}: key={}, value={} (objectId)", i, keyField,
							keyValue);
				} else if (bsonValue.isBoolean()) {
					keyValue = bsonValue.asBoolean().getValue();
					logger.debug("Generated split point at index {}: key={}, value={} (boolean)", i, keyField,
							keyValue);
				} else {
					// For other types, use a safe conversion
					keyValue = BsonValueConverter.convertBsonValueToObject(bsonValue);
					logger.debug("Generated split point at index {}: key={}, value={} (converted)", i, keyField,
							keyValue);
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
					logger.debug("Generated compound key split point at index {}: fields={}, values={}", i,
							shardKeyFields, splitPoint);
				} else {
					logger.warn("Cannot create split point for compound shard key from: {}", splitPointValue);
					continue;
				}
			}

			splitPoints.add(splitPoint);
		}

		logger.debug("Created {} split points for namespace: {}", splitPoints.size(), namespace);
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
						shardKeyValue.getValue(), shardKeyValue.getValue().getClass().getName(), min.getValue(),
						min.getValue().getClass().getName(), max.getValue(), max.getValue().getClass().getName());

				// Verify the chunk really contains this document
				boolean containsDoc = false;
				try {
					// For RawBsonDocument, we need to extract the nested value
					BsonValue shardKeyBson = shardKeyValue.getValue();

					// Extract the actual value from the min/max RawBsonDocuments
					BsonValue minBson = null;
					BsonValue maxBson = null;

					if (min.getValue() instanceof RawBsonDocument) {
						RawBsonDocument minDoc = (RawBsonDocument) min.getValue();
						if (minDoc.containsKey("x")) { // Use your actual shard key field name
							minBson = minDoc.get("x");
						}
					}

					if (max.getValue() instanceof RawBsonDocument) {
						RawBsonDocument maxDoc = (RawBsonDocument) max.getValue();
						if (maxDoc.containsKey("x")) { // Use your actual shard key field name
							maxBson = maxDoc.get("x");
						}
					}

					logger.debug("Extracted values - shardKey: {}, min: {}, max: {}", shardKeyBson, minBson, maxBson);

					// Now do the comparison on the extracted values
					if (minBson != null && maxBson != null) {
						// If all are numeric, do a numeric comparison
						if (shardKeyBson.isNumber() && minBson.isNumber() && maxBson.isNumber()) {
							double shardKeyDouble = shardKeyBson.asNumber().doubleValue();
							double minDouble = minBson.asNumber().doubleValue();
							double maxDouble = maxBson.asNumber().doubleValue();

							containsDoc = shardKeyDouble >= minDouble && shardKeyDouble < maxDouble;

							logger.debug("Numeric comparison: {} >= {} && {} < {} = {}", shardKeyDouble, minDouble,
									shardKeyDouble, maxDouble, containsDoc);
						} else {
							// Use BsonValueWrapper for proper comparison
							BsonValueWrapper shardKeyWrapper = new BsonValueWrapper(shardKeyBson);
							BsonValueWrapper minWrapper = new BsonValueWrapper(minBson);
							BsonValueWrapper maxWrapper = new BsonValueWrapper(maxBson);

							containsDoc = (shardKeyWrapper.compareTo(minWrapper) >= 0
									&& shardKeyWrapper.compareTo(maxWrapper) < 0);

							logger.debug("BsonValueWrapper comparison: {} >= {} && {} < {} = {}", shardKeyWrapper,
									minWrapper, shardKeyWrapper, maxWrapper, containsDoc);
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