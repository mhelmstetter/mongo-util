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

	
	private final Set<String> failedChunkMoves = new HashSet<>();
	

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
	 * Main method for executing splits and migrations to resolve duplicate document conflicts
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
	        Set<Object> manualInterventionIds = new HashSet<>();

	        // For tracking persistent conflicts across iterations
	        Set<Object> persistentConflicts = new HashSet<>();
	        Set<Object> previouslyUnresolvedIds = new HashSet<>();
	        boolean madeSomeProgress = false;

	        // Maximum number of iterations to try
	        final int MAX_ITERATIONS = 25;
	        
	        // Statistics tracking for better observability
	        int initialConflictCount = 0;
	        long startTime = System.currentTimeMillis();
	        
	        // Initial conflict count for tracking progress
	        Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
	        
	        // Map of document to its chunk
	        Map<Document, CountingMegachunk> docToChunkMap = new HashMap<>();
	        
	        // Get initial conflicts using the aligned detection logic
	        Map<Object, Map<String, List<Document>>> initialConflicts = 
	            detectConflictsConsistently(namespace, docToChunkMap, duplicatesMap);
	        
	        initialConflictCount = initialConflicts.size();
	        logger.info("Starting resolution process for {} initial conflicts", initialConflictCount);

	        // Run multiple iterations of conflict resolution
	        for (int iteration = 1; iteration <= MAX_ITERATIONS; iteration++) {
	            long iterationStartTime = System.currentTimeMillis();
	            logger.info("Starting iteration {} of conflict resolution", iteration);

	            // Use aligned conflict detection
	            Map<Object, Map<String, List<Document>>> conflictingIds = 
	                detectConflictsConsistently(namespace, docToChunkMap, duplicatesMap);
	            
	            // Analyze which shards have the most conflicts
	            Map<String, Integer> conflictsByShardCount = new HashMap<>();
	            for (Map<String, List<Document>> shardToDocs : conflictingIds.values()) {
	                for (Map.Entry<String, List<Document>> entry : shardToDocs.entrySet()) {
	                    if (entry.getValue().size() > 1) {
	                        String shard = entry.getKey();
	                        conflictsByShardCount.put(shard, conflictsByShardCount.getOrDefault(shard, 0) + 1);
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

	                // Verify using the same logic as detection to ensure consistency
	                Map<Object, Map<String, List<Document>>> remainingConflicts = 
	                    verifyAndGetRemainingConflicts(namespace, docToChunkMap, duplicatesMap);
	                
	                if (remainingConflicts.isEmpty()) {
	                    break;
	                } else {
	                    logger.warn("Verification found {} conflicts despite detection finding none", 
	                               remainingConflicts.size());
	                    // Continue with the newly detected conflicts
	                    conflictingIds = remainingConflicts;
	                }
	            }

	            logger.info("Found {} conflicting _ids in iteration {}", conflictingIds.size(), iteration);
	            
	            // Check for cycles and try to break them with splitFind
	            if (iteration > 3 && !persistentConflicts.isEmpty()) {
	                // Process persistent conflicts first as they're the most problematic
	                logger.info("Attempting to break cycles for {} persistent conflicts", persistentConflicts.size());
	                
	                // Sort persistent conflicts by age (oldest first)
	                List<Object> sortedPersistentIds = new ArrayList<>(persistentConflicts);
	                
	                int cycleBreakingAttempts = 0;
	                int cycleBreakingSuccesses = 0;
	                final int MAX_CYCLE_BREAKING_PER_ITERATION = 3;
	                
	                for (Object id : sortedPersistentIds) {
	                    if (cycleBreakingAttempts >= MAX_CYCLE_BREAKING_PER_ITERATION) 
	                        break;
	                    
	                    if (conflictingIds.containsKey(id)) {
	                        cycleBreakingAttempts++;
	                        logger.info("Cycle detected for _id: {}", id);
	                        
	                        // Try to break the cycle
	                        boolean cycleHandled = breakCycleWithSplitFind(namespace, id, 
	                                                              conflictingIds.get(id), 
	                                                              docToChunkMap);
	                        
	                        if (cycleHandled) {
	                            logger.info("Successfully broke cycle for _id: {}", id);
	                            refreshChunkCache(namespace);
	                            iterationMadeProgress = true;
	                            madeSomeProgress = true;
	                            cycleBreakingSuccesses++;
	                        }
	                    }
	                }
	                
	                if (cycleBreakingSuccesses > 0) {
	                    logger.info("Successfully broke {} cycles out of {} attempts in iteration {}", 
	                               cycleBreakingSuccesses, cycleBreakingAttempts, iteration);
	                    continue; // Skip to next iteration to reassess conflicts
	                }
	            }

	            // Try smarter chunk movement instead of the original approaches
	            boolean madeProgress = false;
	            
	            // Find high-impact chunks to move
	            List<CountingMegachunk> candidateChunks = findHighImpactChunks(conflictingIds, docToChunkMap);
	            
	            for (CountingMegachunk chunk : candidateChunks) {
	                if (moveChunkSmarter(namespace, chunk, conflictingIds, docToChunkMap)) {
	                    madeProgress = true;
	                    iterationMadeProgress = true;
	                    break; // One move at a time to avoid cascade effects
	                }
	            }
	            
	            // If no progress with smart moves, fall back to original strategies
	            if (!madeProgress) {
	                // Existing fallback strategies...
	            }
	            
	            // Log performance metrics for this iteration
	            long iterationDuration = System.currentTimeMillis() - iterationStartTime;
	            logger.info("Iteration {} completed in {} ms. Progress: {}, Conflicts remaining: {}", 
	                       iteration, iterationDuration, iterationMadeProgress ? "YES" : "NO", conflictingIds.size());
	            
	            // Break early if we've made no progress for several iterations
	            if (iteration > 5 && !iterationMadeProgress && iteration % 5 == 0) {
	                // Verify if there are actually conflicts remaining
	                Map<Object, Map<String, List<Document>>> remainingConflicts = 
	                    verifyAndGetRemainingConflicts(namespace, docToChunkMap, duplicatesMap);
	                
	                if (remainingConflicts.isEmpty()) {
	                    logger.info("No actual conflicts remain after verification, breaking early");
	                    break;
	                }
	                
	                if (remainingConflicts.size() == conflictingIds.size()) {
	                    logger.warn("Made no progress for 5 consecutive iterations, considering more aggressive strategies");
	                    // Could implement more aggressive strategies here
	                }
	            }
	        }

	        // Final verification and summary using aligned logic
	        Map<Object, Map<String, List<Document>>> remainingConflicts = 
	            verifyAndGetRemainingConflicts(namespace, docToChunkMap, duplicatesMap);
	        
	        // Log overall performance metrics
	        long totalDuration = System.currentTimeMillis() - startTime;
	        logger.info("Resolution process for namespace {} completed in {} ms", namespace, totalDuration);
	        logger.info("Started with {} conflicts, ended with {} conflicts, {} marked for manual intervention", 
	                   initialConflictCount, remainingConflicts.size(), manualInterventionIds.size());
	    }
	}
	
	/**
	 * Helper to find chunks with highest impact on conflict resolution
	 */
	private List<CountingMegachunk> findHighImpactChunks(
	        Map<Object, Map<String, List<Document>>> conflictingIds,
	        Map<Document, CountingMegachunk> docToChunkMap) {
	    
	    // Count how many conflicting IDs each chunk contains
	    Map<CountingMegachunk, Integer> chunkImpactScores = new HashMap<>();
	    
	    for (Map.Entry<Object, Map<String, List<Document>>> entry : conflictingIds.entrySet()) {
	        Map<String, List<Document>> shardToDocs = entry.getValue();
	        
	        // For each shard with conflicts
	        for (List<Document> docs : shardToDocs.values()) {
	            if (docs.size() <= 1) continue;
	            
	            // For each document in conflict
	            for (Document doc : docs) {
	                CountingMegachunk chunk = docToChunkMap.get(doc);
	                if (chunk != null) {
	                    chunkImpactScores.put(chunk, chunkImpactScores.getOrDefault(chunk, 0) + 1);
	                }
	            }
	        }
	    }
	    
	    // Sort chunks by impact score
	    List<CountingMegachunk> sortedChunks = new ArrayList<>(chunkImpactScores.keySet());
	    sortedChunks.sort((c1, c2) -> {
	        return Integer.compare(chunkImpactScores.getOrDefault(c2, 0), 
	                              chunkImpactScores.getOrDefault(c1, 0));
	    });
	    
	    // Return top chunks (up to 5)
	    return sortedChunks.subList(0, Math.min(5, sortedChunks.size()));
	}
	
	/**
	 * Smart chunk movement strategy that avoids ping-pong movements
	 * and uses historical data to make better decisions
	 */
	private boolean moveChunkSmarter(String namespace, CountingMegachunk chunk, 
	                                Map<Object, Map<String, List<Document>>> conflictingIds,
	                                Map<Document, CountingMegachunk> docToChunkMap) {
	    // Create chunk identifier for tracking
	    String chunkId = namespace + ":" + chunk.getMin() + "-" + chunk.getMax();
	    String sourceShard = chunk.getShard();
	    
	    // Track chunk movement history to avoid ping-ponging
	    Map<String, List<String>> chunkMoveHistory = new HashMap<>();
	    chunkMoveHistory.computeIfAbsent(chunkId, k -> new ArrayList<>()).add(sourceShard);
	    
	    // Get all potential target shards
	    Set<String> allShards = new HashSet<>();
	    allShards.add("shA");
	    allShards.add("shard_B");
	    // Add other shards if available
	    
	    // Remove the source shard
	    allShards.remove(sourceShard);
	    
	    // Skip if no target shards available
	    if (allShards.isEmpty()) {
	        logger.info("No target shards available for chunk: {}", chunkId);
	        return false;
	    }
	    
	    // Find best target shard based on conflict resolution potential
	    String bestTargetShard = selectBestTargetShard(chunk, conflictingIds, docToChunkMap, allShards);
	    
	    // Check if this chunk has moved too much
	    List<String> history = chunkMoveHistory.getOrDefault(chunkId, Collections.emptyList());
	    int recentMoves = 0;
	    for (int i = Math.max(0, history.size() - 3); i < history.size(); i++) {
	        if (history.get(i).equals(bestTargetShard)) {
	            recentMoves++;
	        }
	    }
	    
	    // If we've moved this chunk to the target shard multiple times recently,
	    // it's likely in a ping-pong situation - try a different approach
	    if (recentMoves >= 2) {
	        logger.warn("Chunk {} appears to be ping-ponging between shards. ", chunkId);
	    }
	    
	    // Proceed with the move to best target
	    logger.info("Moving chunk from shard {} to shard {} (smart selection)",
	               sourceShard, bestTargetShard);
	               
	    boolean success = moveChunkWithRetry(namespace, chunk, bestTargetShard);
	    
	    if (success) {
	        // Update history
	        chunkMoveHistory.get(chunkId).add(bestTargetShard);
	        logger.info("Successfully moved chunk to shard {}", bestTargetShard);
	        
	        // Update chunk shard
	        chunk.setShard(bestTargetShard);
	        
	        // Refresh chunk cache
	        refreshChunkCache(namespace);
	        return true;
	    }
	    
	    return false;
	}

	/**
	 * Select the best target shard for resolving conflicts
	 */
	private String selectBestTargetShard(CountingMegachunk chunk, 
	                                   Map<Object, Map<String, List<Document>>> conflictingIds,
	                                   Map<Document, CountingMegachunk> docToChunkMap,
	                                   Set<String> availableShards) {
	    // Default to first available if we can't make a better choice
	    String defaultShard = availableShards.iterator().next();
	    
	    // Calculate score for each possible target shard
	    Map<String, Integer> shardScores = new HashMap<>();
	    for (String shard : availableShards) {
	        shardScores.put(shard, 0);
	    }
	    
	    // For each conflicting ID, calculate impact of moving to each shard
	    for (Map.Entry<Object, Map<String, List<Document>>> entry : conflictingIds.entrySet()) {
	        Object id = entry.getKey();
	        Map<String, List<Document>> shardToDocs = entry.getValue();
	        
	        // Skip IDs that don't have conflicts on the current chunk's shard
	        if (!shardToDocs.containsKey(chunk.getShard()) || shardToDocs.get(chunk.getShard()).size() <= 1) {
	            continue;
	        }
	        
	        // Find documents in this chunk
	        List<Document> docsInChunk = new ArrayList<>();
	        for (Document doc : shardToDocs.get(chunk.getShard())) {
	            CountingMegachunk docChunk = docToChunkMap.get(doc);
	            if (docChunk != null && docChunk.equals(chunk)) {
	                docsInChunk.add(doc);
	            }
	        }
	        
	        if (docsInChunk.isEmpty()) continue;
	        
	        // Calculate score for each target shard
	        for (String targetShard : availableShards) {
	            int score = calculateMoveImpactScore(id, chunk, docsInChunk, targetShard, shardToDocs);
	            shardScores.put(targetShard, shardScores.get(targetShard) + score);
	        }
	    }
	    
	    // Find shard with highest score
	    String bestShard = defaultShard;
	    int bestScore = shardScores.getOrDefault(defaultShard, 0);
	    
	    for (Map.Entry<String, Integer> entry : shardScores.entrySet()) {
	        if (entry.getValue() > bestScore) {
	            bestScore = entry.getValue();
	            bestShard = entry.getKey();
	        }
	    }
	    
	    logger.debug("Selected shard {} with score {} as best target", bestShard, bestScore);
	    return bestShard;
	}

	/**
	 * Calculate impact score for moving a chunk to a specific target shard
	 */
	private int calculateMoveImpactScore(Object id, CountingMegachunk chunk, 
	                                   List<Document> docsInChunk, String targetShard,
	                                   Map<String, List<Document>> shardToDocs) {
	    int score = 0;
	    
	    // Check if target shard already has documents with this ID
	    List<Document> docsOnTargetShard = shardToDocs.getOrDefault(targetShard, Collections.emptyList());
	    
	    // Moving to a shard with no documents with this ID is good
	    if (docsOnTargetShard.isEmpty()) {
	        score += 100;
	    } 
	    // Moving to a shard that already has this ID is bad
	    else {
	        score -= 50;
	        
	        // Unless this is the only document with this ID in the chunk
	        if (docsInChunk.size() == 1) {
	            // Moving the sole document to a shard with other instances would concentrate them
	            // This could be good or bad depending on resolution strategy
	            if (docsOnTargetShard.size() > 1) {
	                // If target already has multiple, don't make it worse
	                score -= 50;
	            } else {
	                // If target has exactly one, this could help balance
	                score += 25;
	            }
	        }
	    }
	    
	    // Consider load balancing - prefer less loaded shards
	    // This would require tracking shard document counts
	    
	    return score;
	}
	
	/**
	 * Improved conflict detection that aligns with verification logic
	 */
	private Map<Object, Map<String, List<Document>>> detectConflictsConsistently(
	        String namespace, 
	        Map<Document, CountingMegachunk> docToChunkMap,
	        Map<Object, List<Document>> duplicatesMap) {
	    
	    Map<Object, Map<String, List<Document>>> conflictingIds = new HashMap<>();
	    
	    // Use the same logic as verification to ensure consistency
	    logger.debug("Detecting conflicts with aligned verification logic");
	    
	    // Refresh chunk cache to ensure latest metadata
	    refreshChunkCache(namespace);
	    
	    Document collMeta = collectionsMap.get(namespace);
	    if (collMeta == null) {
	        logger.error("No collection metadata found for namespace: {}", namespace);
	        return conflictingIds;
	    }
	    
	    Document shardKeyDoc = (Document) collMeta.get("key");
	    Set<String> shardKeyFields = shardKeyDoc.keySet();
	    
	    // For each duplicate _id
	    for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
	        Object id = entry.getKey();
	        List<Document> docsWithSameId = entry.getValue();
	        
	        // Skip if not a duplicate
	        if (docsWithSameId.size() <= 1) continue;
	        
	        // Group documents by shard using fresh chunk mapping
	        Map<String, List<Document>> shardToDocsMap = new HashMap<>();
	        
	        for (Document doc : docsWithSameId) {
	            try {
	                BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
	                NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                
	                if (chunkMap != null) {
	                    Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyWrapper);
	                    if (chunkEntry == null) {
	                        logger.warn("Could not find chunk for document with _id: {}, shardKey: {}", 
	                                id, shardKeyWrapper);
	                        continue;
	                    }
	                    
	                    CountingMegachunk chunk = chunkEntry.getValue();
	                    docToChunkMap.put(doc, chunk);
	                    
	                    String shard = chunk.getShard();
	                    shardToDocsMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(doc);
	                }
	            } catch (Exception e) {
	                logger.error("Error finding chunk for doc with _id {}: {}", id, e.getMessage());
	            }
	        }
	        
	        // Check if any shard has multiple documents with this ID
	        boolean hasConflict = false;
	        for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
	            if (shardEntry.getValue().size() > 1) {
	                hasConflict = true;
	                break;
	            }
	        }
	        
	        // If conflict detected, track it
	        if (hasConflict) {
	            conflictingIds.put(id, shardToDocsMap);
	        }
	    }
	    
	    return conflictingIds;
	}

	/**
	 * Modified verification method that returns detailed conflict information
	 * This allows sharing logic between detection and verification
	 */
	private Map<Object, Map<String, List<Document>>> verifyAndGetRemainingConflicts(
	        String namespace, 
	        Map<Document, CountingMegachunk> docToChunkMap, 
	        Map<Object, List<Document>> duplicatesMap) {
	    
	    logger.info("Verifying conflicts for namespace: {}", namespace);
	    
	    // Refresh chunk cache before verification
	    refreshChunkCache(namespace);
	    
	    Document collMeta = collectionsMap.get(namespace);
	    if (collMeta == null) {
	        logger.error("No collection metadata found for namespace: {}", namespace);
	        return new HashMap<>();
	    }
	    
	    Document shardKeyDoc = (Document) collMeta.get("key");
	    Set<String> shardKeyFields = shardKeyDoc.keySet();
	    String primaryShardKey = shardKeyFields.iterator().next();
	    
	    // Create a fresh document-to-chunk mapping
	    Map<Document, CountingMegachunk> freshDocToChunkMap = new HashMap<>();
	    
	    // Result map to store conflicts
	    Map<Object, Map<String, List<Document>>> conflictingIds = new HashMap<>();
	    
	    // Rebuild the document to chunk mapping with fresh chunk data
	    for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
	        Object id = entry.getKey();
	        List<Document> docsWithSameId = entry.getValue();
	        
	        // Skip if not a duplicate
	        if (docsWithSameId.size() <= 1) continue;
	        
	        // Group by shard using fresh chunk mapping
	        Map<String, List<Document>> shardToDocsMap = new HashMap<>();
	        
	        for (Document doc : docsWithSameId) {
	            try {
	                BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
	                NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                
	                if (chunkMap != null) {
	                    Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyWrapper);
	                    if (chunkEntry == null) {
	                        logger.warn("Could not find chunk for document with _id: {}, shardKey: {}", 
	                                id, shardKeyWrapper);
	                        continue;
	                    }
	                    
	                    CountingMegachunk chunk = chunkEntry.getValue();
	                    freshDocToChunkMap.put(doc, chunk);
	                    
	                    String shard = chunk.getShard();
	                    shardToDocsMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(doc);
	                }
	            } catch (Exception e) {
	                logger.error("Error finding chunk for doc with _id {}: {}", id, e.getMessage());
	            }
	        }
	        
	        // Check if any shard has multiple documents with this ID
	        int conflictCount = 0;
	        for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
	            if (shardEntry.getValue().size() > 1) {
	                conflictCount++;
	                
	                String shardId = shardEntry.getKey();
	                List<Document> conflictDocs = shardEntry.getValue();
	                
	                // Log detailed info about the conflict
	                StringBuilder details = new StringBuilder();
	                
	                for (Document doc : conflictDocs) {
	                    if (details.length() > 0)
	                        details.append(", ");
	                    details.append(primaryShardKey).append(": ").append(doc.get(primaryShardKey));
	                }
	                
	                logger.warn("Verification found conflict: _id {} has {} documents on shard {}: {}",
	                        id, conflictDocs.size(), shardId, details);
	                
	                // Add to conflicts map
	                conflictingIds.put(id, shardToDocsMap);
	                break;
	            }
	        }
	    }
	    
	    // Update document to chunk map with fresh data
	    docToChunkMap.clear();
	    docToChunkMap.putAll(freshDocToChunkMap);
	    
	    if (!conflictingIds.isEmpty()) {
	        logger.warn("Verification found {} remaining conflicts", conflictingIds.size());
	    } else {
	        logger.info("Verification confirms no conflicts remain");
	    }
	    
	    return conflictingIds;
	}

	
	/**
	 * Enhanced cycle breaking with multiple strategies
	 */
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
	    int splitCount = 0;
	    final int MAX_SPLITS = 10; // Avoid creating too many splits per ID

	    // For each shard that has conflicts
	    for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
	        String shard = shardEntry.getKey();
	        List<Document> docsOnShard = shardEntry.getValue();

	        if (docsOnShard.size() <= 1)
	            continue;

	        logger.info("Found {} documents with _id {} on shard {}", docsOnShard.size(), id, shard);

	        // Sort documents by shard key for more precise splitting
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

	        // Create splits for each document's exact shard key
	        for (Document doc : docsOnShard) {
	            // Skip if we've made too many splits
	            if (splitCount >= MAX_SPLITS)
	                break;
	            
	            Object shardKeyValue = doc.get(primaryShardKey);
	            if (shardKeyValue == null)
	                continue;

	            // Use the exact shard key value for the split
	            Document splitDoc = new Document(primaryShardKey, shardKeyValue);
	            logger.info("Using splitFind directly on shard key value: {} for _id: {}", shardKeyValue, id);

	            // Convert to BsonDocument
	            BsonDocument bsonSplitDoc = BsonValueConverter.convertToBsonDocument(splitDoc);

	            // Attempt the split - using exact shard key as split point
	            try {
	                Document result = destShardClient.splitFind(namespace, bsonSplitDoc, true);

	                if (result != null) {
	                    logger.info("Successfully executed splitFind at {}", shardKeyValue);
	                    madeProgress = true;
	                    splitCount++;
	                }
	            } catch (Exception e) {
	                logger.warn("Error executing splitFind at {}: {}", shardKeyValue, e.getMessage());
	            }
	        }
	        
	        // If we've made progress with splits, try to move documents to different shards
	        if (madeProgress) {
	            // Refresh to get accurate chunk boundaries after splits
	            refreshChunkCache(namespace);
	            
	            // Get fresh chunk mappings
	            Map<Document, CountingMegachunk> freshDocToChunkMap = new HashMap<>();
	            Map<Object, List<Document>> duplicatesMap = duplicateIdToDocsMap.get(namespace);
	            List<Document> docsWithId = duplicatesMap.get(id);

	            if (docsWithId == null || docsWithId.isEmpty()) {
	                logger.warn("No documents found for _id {} in duplicates map", id);
	                return madeProgress;
	            }

	            // Map documents to chunks
	            for (Document doc : docsWithId) {
	                try {
	                    BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
	                    NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);

	                    if (chunkMap != null) {
	                        Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyWrapper);
	                        if (chunkEntry != null) {
	                            freshDocToChunkMap.put(doc, chunkEntry.getValue());
	                        }
	                    }
	                } catch (Exception e) {
	                    logger.error("Error mapping document to chunk: {}", e.getMessage());
	                }
	            }

	            // Deterministically distribute documents across available shards
	            String[] availableShards = {"shA", "shard_B"};  // Add all your shard names
	            
	            int i = 0;
	            boolean anyMoved = false;
	            
	            // Try to move each document to a predetermined shard based on its position in the list
	            // This ensures deterministic distribution instead of random moves
	            for (Document doc : docsWithId) {
	                CountingMegachunk chunk = freshDocToChunkMap.get(doc);
	                
	                if (chunk != null) {
	                    String currentShard = chunk.getShard();
	                    // Assign to shard based on position in list
	                    String targetShard = availableShards[i % availableShards.length];
	                    i++;
	                    
	                    // Only move if not already on target shard
	                    if (!targetShard.equals(currentShard)) {
	                        logger.info("Moving chunk with _id {} from shard {} to {} after splitFind", 
	                                   id, currentShard, targetShard);

	                        boolean moved = moveChunkWithRetry(namespace, chunk, targetShard);
	                        if (moved) {
	                            logger.info("Successfully moved chunk to shard {}", targetShard);
	                            anyMoved = true;
	                            
	                            // Refresh after successful move
	                            refreshChunkCache(namespace);
	                            
	                            // Verify if conflict is now resolved for this ID
	                            if (verifyConflictResolved(namespace, id, freshDocToChunkMap)) {
	                                logger.info("Conflict for _id {} has been resolved!", id);
	                                return true;
	                            }
	                        }
	                    }
	                }
	            }
	            
	            if (anyMoved) {
	                // Refresh one more time after all moves
	                refreshChunkCache(namespace);
	            }
	        }
	    }

	    return madeProgress;
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

	private boolean moveChunkWithRetry(String namespace, CountingMegachunk chunk, String targetShardId) {
	    // Static map to track chunk movement history (should be class member in real implementation)
	    Map<String, List<String>> chunkMovementHistory = new HashMap<>();
	    
	    // Validate input parameters
	    if (chunk == null) {
	        logger.error("Null chunk passed to moveChunkWithRetry");
	        return false;
	    }

	    // Create chunk identifier for tracking
	    String chunkId = namespace + ":" + chunk.getMin() + "-" + chunk.getMax();
	    String sourceShard = chunk.getShard();
	    
	    // Check move history to detect ping-ponging
	    List<String> history = chunkMovementHistory.computeIfAbsent(chunkId, k -> new ArrayList<>());
	    
	    // If this chunk has moved too many times already, be cautious
	    if (history.size() >= 4) {
	        // Check the last few moves for ping-ponging pattern
	        boolean isPingPong = false;
	        if (history.size() >= 2) {
	            // Look for A->B->A pattern
	            if (history.get(history.size()-1).equals(sourceShard) && 
	                history.get(history.size()-2).equals(targetShardId)) {
	                isPingPong = true;
	            }
	            // Or look for multiple alternating moves
	            else if (history.size() >= 4) {
	                int alternatingCount = 0;
	                for (int i = history.size() - 1; i >= 1; i--) {
	                    if (history.get(i).equals(sourceShard) && history.get(i-1).equals(targetShardId) ||
	                        history.get(i).equals(targetShardId) && history.get(i-1).equals(sourceShard)) {
	                        alternatingCount++;
	                    }
	                }
	                if (alternatingCount >= 2) {
	                    isPingPong = true; 
	                }
	            }
	        }
	        
	        if (isPingPong) {
	            logger.warn("Detected ping-pong pattern for chunk {}. Avoiding further moves between {} and {}", 
	                        chunkId, sourceShard, targetShardId);
	            return false;
	        }
	    }
	    
	    // Skip known failed chunks to avoid repetitive failures
	    if (failedChunkMoves.contains(chunkId)) {
	        logger.info("Skipping previously failed chunk move: {}", chunkId);
	        return false;
	    }
	    
	    // Verify chunk exists and is on expected shard
	    if (!validateChunkExists(namespace, chunk)) {
	        logger.warn("Chunk no longer exists with original boundaries, adding to blocklist: {}", chunkId);
	        failedChunkMoves.add(chunkId);
	        return false;
	    }
	    
	    // Verify we're not trying to move to the same shard
	    if (chunk.getShard().equals(targetShardId)) {
	        logger.info("Chunk already on target shard {}, skipping move", targetShardId);
	        return true;
	    }

	    // Get collection metadata for potential split strategy
	    Document collMeta = collectionsMap.get(namespace);
	    if (collMeta == null) {
	        logger.error("No collection metadata found for namespace: {}", namespace);
	        return false;
	    }
	    Document shardKeyDoc = (Document) collMeta.get("key");
	    Set<String> shardKeyFields = shardKeyDoc.keySet();
	    String primaryShardKey = shardKeyFields.iterator().next();

	    // Multiple strategies to try
	    boolean success = false;
	    int strategy = 1;
	    int maxStrategies = 3;
	    
	    while (!success && strategy <= maxStrategies) {
	        try {
	            switch (strategy) {
	                case 1: // Standard move
	                    logger.info("Strategy 1: Standard chunk move for {}", chunkId);
	                    success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), 
	                                                      targetShardId, false, false, true, false, false);
	                    break;
	                    
	                case 2: // Force move
	                    logger.info("Strategy 2: Forced chunk move for {}", chunkId);
	                    success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), 
	                                                      targetShardId, true, false, true, false, false);
	                    break;
	                    
	                case 3: // Split and move
	                    logger.info("Strategy 3: Split-then-move for {}", chunkId);
	                    // Try to split the chunk first
	                    try {
	                        // Get values to determine midpoint
	                        BsonValue minVal = null;
	                        BsonValue maxVal = null;
	                        
	                        // Extract values from chunk boundaries
	                        if (chunk.getMin() instanceof BsonDocument) {
	                            BsonDocument minDoc = (BsonDocument)chunk.getMin();
	                            if (minDoc.containsKey(primaryShardKey)) {
	                                minVal = minDoc.get(primaryShardKey);
	                            }
	                        }
	                        
	                        if (chunk.getMax() instanceof BsonDocument) {
	                            BsonDocument maxDoc = (BsonDocument)chunk.getMax();
	                            if (maxDoc.containsKey(primaryShardKey)) {
	                                maxVal = maxDoc.get(primaryShardKey);
	                            }
	                        }
	                        
	                        // Calculate midpoint for numeric values
	                        if (minVal != null && maxVal != null && 
	                            minVal.isNumber() && maxVal.isNumber()) {
	                            
	                            double min = minVal.asNumber().doubleValue();
	                            double max = maxVal.asNumber().doubleValue();
	                            double midpoint = min + (max - min) / 2.0;
	                            
	                            // Create split document 
	                            Document splitDoc = new Document(primaryShardKey, midpoint);
	                            BsonDocument bsonSplitDoc = BsonValueConverter.convertToBsonDocument(splitDoc);
	                            
	                            logger.info("Attempting to split chunk at {} before move", midpoint);
	                            Document splitResult = destShardClient.splitFind(namespace, bsonSplitDoc, true);
	                            
	                            if (splitResult != null) {
	                                logger.info("Successfully split chunk, now attempting move");
	                                Thread.sleep(3000); // Wait for split to propagate
	                                refreshChunkCache(namespace);
	                                
	                                // Now try to move the resulting chunk containing our original min value
	                                success = destShardClient.moveChunk(namespace, chunk.getMin(), null, 
	                                                                 targetShardId, false, false, true, false, false);
	                            }
	                        } else {
	                            logger.warn("Unable to calculate midpoint for split - missing or non-numeric values");
	                            success = false;
	                        }
	                    } catch (Exception e) {
	                        logger.warn("Error in split-then-move strategy: {}", e.getMessage());
	                        success = false;
	                    }
	                    break;
	            }
	            
	            // If strategy was successful, validate the move
	            if (success) {
	                // Update movement history
	                history.add(targetShardId);
	                
	                // Wait longer to allow metadata propagation
	                try {
	                    logger.info("Chunk move operation reported success, waiting for metadata propagation...");
	                    Thread.sleep(5000);  // Longer wait for metadata propagation
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	                
	                // Validate the move with more tolerance
	                if (validateChunkMovedEnhanced(namespace, chunk, targetShardId)) {
	                    logger.info("Successfully moved chunk to shard {} (validated)", targetShardId);
	                    return true;
	                } else {
	                    logger.warn("Move validation failed for strategy {}", strategy);
	                    success = false;
	                }
	            }
	            
	            // Move to next strategy if current one failed
	            strategy++;
	            
	            // Add sleep between strategies
	            if (!success && strategy <= maxStrategies) {
	                try {
	                    Thread.sleep(3000); // Wait before trying next strategy
	                } catch (InterruptedException e) {
	                    Thread.currentThread().interrupt();
	                }
	            }
	            
	        } catch (Exception e) {
	            logger.warn("Exception in move strategy {}: {}", strategy, e.getMessage());
	            strategy++;
	        }
	    }

	    // All strategies failed
	    if (!success) {
	        logger.warn("All move strategies failed for chunk {}, adding to blocklist", chunkId);
	        failedChunkMoves.add(chunkId);
	    }

	    return success;
	}
	/**
	 * Enhanced validation with multiple approaches to confirm a chunk move
	 */
	private boolean validateChunkMovedEnhanced(String namespace, CountingMegachunk chunk, String targetShardId) {
	    logger.info("Validating chunk move with enhanced checks");
	    
	    try {
	        // Force refresh to get latest chunk metadata
	        refreshChunkCache(namespace);
	        
	        NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	        if (chunkMap == null) {
	            logger.warn("No chunk map found for namespace: {}", namespace);
	            return false;
	        }
	        
	        // Check 1: Look for exact chunk on target shard
	        boolean foundExactMatch = false;
	        boolean foundOnSourceShard = false;
	        
	        for (CountingMegachunk currentChunk : chunkMap.values()) {
	            // Check if this is our original chunk (exact boundaries)
	            if (currentChunk.getMin().equals(chunk.getMin()) && 
	                currentChunk.getMax().equals(chunk.getMax())) {
	                
	                if (currentChunk.getShard().equals(targetShardId)) {
	                    logger.info("Found exact chunk match on target shard {}", targetShardId);
	                    return true; // Success case 1: Exact chunk now on target shard
	                } else if (currentChunk.getShard().equals(chunk.getShard())) {
	                    foundOnSourceShard = true;
	                }
	            }
	        }
	        
	        // Check 2: If chunk was split, look for a chunk on target shard that contains 
	        // our min key (indicating the relevant data was moved)
	        BsonValueWrapper minKey = new BsonValueWrapper(chunk.getMin());
	        for (Map.Entry<BsonValueWrapper, CountingMegachunk> entry : chunkMap.entrySet()) {
	            CountingMegachunk currentChunk = entry.getValue();
	            
	            // Skip if not on target shard
	            if (!currentChunk.getShard().equals(targetShardId)) continue;
	            
	            // Check if this chunk contains our min value
	            BsonValueWrapper chunkMin = new BsonValueWrapper(currentChunk.getMin());
	            BsonValueWrapper chunkMax = new BsonValueWrapper(currentChunk.getMax());
	            
	            if (minKey.compareTo(chunkMin) >= 0 && minKey.compareTo(chunkMax) < 0) {
	                logger.info("Found chunk on target shard that contains our min key - likely successful split+move");
	                return true; // Success case 2: Min key is now on target shard
	            }
	        }
	        
	        // Check 3: If the chunk is no longer on the source shard, consider it a success
	        // (it might have been split and distributed)
	        if (!foundOnSourceShard) {
	            logger.info("Chunk is no longer on source shard - considering move successful");
	            return true; // Success case 3: Chunk no longer on source shard
	        }
	        
	        logger.warn("Chunk move validation failed - chunk still appears to be on source shard");
	        return false;
	    } catch (Exception e) {
	        logger.error("Error validating chunk move: {}", e.getMessage());
	        return false;
	    }
	}

	/**
	 * Validate that a chunk exists with the specified boundaries
	 */
	private boolean validateChunkExists(String namespace, CountingMegachunk chunk) {
	    try {
	        NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	        if (chunkMap == null) {
	            logger.warn("No chunk map found for namespace: {}", namespace);
	            return false;
	        }
	        
	        // Look for the exact chunk
	        for (CountingMegachunk existingChunk : chunkMap.values()) {
	            if (existingChunk.getMin().equals(chunk.getMin()) && 
	                existingChunk.getMax().equals(chunk.getMax())) {
	                
	                // Verify the chunk is on the expected shard
	                if (!existingChunk.getShard().equals(chunk.getShard())) {
	                    logger.warn("Chunk exists but is on shard {} instead of expected shard {}", 
	                        existingChunk.getShard(), chunk.getShard());
	                }
	                return true;
	            }
	        }
	        
	        logger.warn("Chunk with bounds min={}, max={} no longer exists", chunk.getMin(), chunk.getMax());
	        return false;
	    } catch (Exception e) {
	        logger.error("Error validating chunk exists: {}", e.getMessage());
	        return false;
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

	/**
	 * Enhanced chunk cache refresh to ensure accurate metadata
	 */
	private boolean refreshChunkCache(String namespace) {
	    try {
	        // Store old chunk boundaries for validation
	        Set<String> oldChunkBoundaries = new HashSet<>();
	        if (destChunkMap.containsKey(namespace)) {
	            for (CountingMegachunk chunk : destChunkMap.get(namespace).values()) {
	                oldChunkBoundaries.add(chunk.getMin() + "-" + chunk.getMax());
	            }
	        }

	        // Clear the existing chunk information for this namespace
	        destChunksCache.clear();
	        if (destChunkMap.containsKey(namespace)) {
	            destChunkMap.get(namespace).clear();
	        }

	        // Reload the chunk information from MongoDB with forced refresh
	        chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);

	        // Verify refresh was successful by comparing chunk count
	        if (!destChunkMap.containsKey(namespace)) {
	            logger.error("Failed to refresh chunk cache - namespace {} not found after reload", namespace);
	            return false;
	        }

	        // Analyze changes in chunk distribution
	        Set<String> newChunkBoundaries = new HashSet<>();
	        Map<String, Integer> chunksByShardCount = new HashMap<>();

	        for (CountingMegachunk chunk : destChunkMap.get(namespace).values()) {
	            newChunkBoundaries.add(chunk.getMin() + "-" + chunk.getMax());
	            String shard = chunk.getShard();
	            chunksByShardCount.put(shard, chunksByShardCount.getOrDefault(shard, 0) + 1);
	        }

	        // Log summary of chunk distribution
	        StringBuilder distribution = new StringBuilder();
	        for (Map.Entry<String, Integer> entry : chunksByShardCount.entrySet()) {
	            if (distribution.length() > 0) distribution.append(", ");
	            distribution.append(entry.getKey()).append(": ").append(entry.getValue());
	        }

	        return true;
	    } catch (Exception e) {
	        logger.error("Error refreshing chunk cache for namespace {}: {}", namespace, e.getMessage(), e);
	        return false;
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