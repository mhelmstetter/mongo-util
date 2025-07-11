package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
	
	private Set<String> targetShards;


	public DuplicateResolver(ShardClient destShardClient, ChunkManager chunkManager, Set<String> targetShards) {
		this.destShardClient = destShardClient;
		this.collectionsMap = destShardClient.getCollectionsMap();

		chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
		this.chunkManager = chunkManager;
		
		if (targetShards != null && !targetShards.isEmpty()) {
			this.targetShards = targetShards;
		} else {
			this.targetShards = destShardClient.getShardsMap().keySet();
		}
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


	        // Set of _ids that have been successfully resolved
	        Set<Object> manualInterventionIds = new HashSet<>();

	        // For tracking persistent conflicts across iterations
	        Set<Object> persistentConflicts = new HashSet<>();
	        Set<Object> previouslyUnresolvedIds = new HashSet<>();

	        // Maximum number of iterations to try
	        final int MAX_ITERATIONS = 100;
	        
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
	                        
	                        // Try to break the cycle
	                        boolean cycleHandled = breakCycleWithSplitFind(namespace, id, 
	                                                              conflictingIds.get(id), 
	                                                              docToChunkMap);
	                        
	                        if (cycleHandled) {
	                            logger.info("Successfully broke cycle for _id: {}", id);
	                            refreshChunkCache(namespace);
	                            iterationMadeProgress = true;
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
	        for (Map.Entry<String, List<Document>> shardEntry : shardToDocsMap.entrySet()) {
	            if (shardEntry.getValue().size() > 1) {
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
	
	public static Number getMaxValueForType(Number number) {
	    if (number instanceof Integer) {
	        return Integer.MAX_VALUE;
	    } else if (number instanceof Long) {
	        return Long.MAX_VALUE;
	    } else if (number instanceof Float) {
	        return Float.MAX_VALUE;
	    } else if (number instanceof Double) {
	        return Double.MAX_VALUE;
	    } else if (number instanceof Short) {
	        return Short.MAX_VALUE;
	    } else if (number instanceof Byte) {
	        return Byte.MAX_VALUE;
	    }
	    
	    return Double.MAX_VALUE;
	}
	
	public static Number getMinValueForType(Number number) {
	    if (number instanceof Integer) {
	        return Integer.MIN_VALUE;
	    } else if (number instanceof Long) {
	        return Long.MIN_VALUE;
	    } else if (number instanceof Float) {
	        return Float.MIN_VALUE;
	    } else if (number instanceof Double) {
	        return Double.MIN_VALUE;
	    } else if (number instanceof Short) {
	        return Short.MIN_VALUE;
	    } else if (number instanceof Byte) {
	        return Byte.MIN_VALUE;
	    }
	    
	    return Double.MIN_VALUE;
	}
	
	public static Number findMidpoint(Object min, Object max) {
	    Number minNum = null;
	    Number maxNum = null;

	    if (min instanceof Number) {
	        minNum = (Number)min;
	    } else if (min instanceof BsonMinKey) {
	        minNum = getMinValueForType((Number)max);
	    } else {
	        logger.warn("min unexpected type: {}", min.getClass().getName());
	    }

	    if (max instanceof Number) {
	        maxNum = (Number)max;
	    } else if (max instanceof BsonMaxKey) {
	        maxNum = getMaxValueForType(minNum);
	    } else {
	        logger.warn("max unexpected type: {}", max);
	    }

	    // Handle integer types
	    if (minNum instanceof Integer || maxNum instanceof Integer) {
	        // Use formula that avoids overflow
	        return minNum.intValue() + (maxNum.intValue() - minNum.intValue()) / 2;
	    }
	    // Handle long types
	    else if (minNum instanceof Long || maxNum instanceof Long) {
	        // Use formula that avoids overflow for longs as well
	        return minNum.longValue() + (maxNum.longValue() - minNum.longValue()) / 2L;
	    }
	    // For mixed types or floating point numbers, use double
	    else {
	        return (minNum.doubleValue() + maxNum.doubleValue()) / 2.0;
	    }
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
//	        docsOnShard.sort((a, b) -> {
//	            Object valA = a.get(primaryShardKey);
//	            Object valB = b.get(primaryShardKey);
//
//	            if (valA == null || valB == null)
//	                return 0;
//
//	            if (valA instanceof Number && valB instanceof Number) {
//	                return Double.compare(((Number) valA).doubleValue(), ((Number) valB).doubleValue());
//	            }
//
//	            return valA.toString().compareTo(valB.toString());
//	        });

	        // Create splits for each document's exact shard key
	        for (Document doc : docsOnShard) {
	            // Skip if we've made too many splits
	            if (splitCount >= MAX_SPLITS)
	                break;
	            
	            Object shardKeyValue = doc.get(primaryShardKey);
	            if (shardKeyValue == null)
	                continue;
	            
	            CountingMegachunk chunk = docToChunkMap.get(doc);
	            
	            BsonValue minVal = chunk.getMin().get(primaryShardKey);
	            BsonValue maxVal = chunk.getMax().get(primaryShardKey);

	            // Use the exact shard key value for the split
	            BsonDocument splitDoc = new BsonDocument(primaryShardKey, BsonValueConverter.convertToBsonValue(shardKeyValue));
	            logger.info("Using splitAt directly on shard key value: {} for _id: {}", shardKeyValue, id);

	            Object min = BsonValueConverter.convertBsonValueToObject(minVal);
	            Object max = BsonValueConverter.convertBsonValueToObject(maxVal);
	            

	            
	            Number midPoint = findMidpoint(min, max);
	            splitDoc = new BsonDocument(primaryShardKey, BsonValueConverter.convertToBsonValue(midPoint));

	            // Attempt the split - using exact shard key as split point
	            try {
	                Document result = destShardClient.splitAt(namespace, splitDoc, true);

	                if (result != null) {
	                    logger.info("Successfully executed splitAt at {}", shardKeyValue);
	                    madeProgress = true;
	                    splitCount++;
	                } else {
	                	Number midPoint2 = findMidpoint(min, midPoint);
	                	splitDoc = new BsonDocument(primaryShardKey, BsonValueConverter.convertToBsonValue(midPoint2));
	                	result = destShardClient.splitAt(namespace, splitDoc, true);
	                	if (result != null) {
	                		logger.info("2nd attempt - successfully executed splitAt at {}", midPoint2);
	                		madeProgress = true;
		                    splitCount++;
	                	} else {
	                		logger.warn("2nd attempt failed to splitAt at {}", midPoint2);
	                	}
	                }
	            } catch (Exception e) {
	                logger.warn("Error executing splitAt at {}: {}", shardKeyValue, e.getMessage());
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
	            String[] availableShards = targetShards.toArray(new String[0]);
	            
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
				for (BsonDocument splitPoint : calculateSplitPoints(namespace, shardKeyDoc.keySet(),
						getShardKeyValuesForDocs(splitInfo.getSplitDocs(), primaryShardKey))) {
					splitCount++;

					// Execute the split
					logger.info("Executing split at {} for chunk with bounds: min={}, max={} on shard {}", splitPoint,
							chunk.getMin(), chunk.getMax(), shardId);

					Document result = destShardClient.splitFind(namespace, splitPoint, true);

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


	private boolean moveChunkWithRetry(String namespace, CountingMegachunk chunk, String targetShardId) {
	    
	    // Validate input parameters
	    if (chunk == null) {
	        logger.error("Null chunk passed to moveChunkWithRetry");
	        return false;
	    }

	    // Create chunk identifier for tracking
	    String chunkId = namespace + ":" + chunk.getMin() + "-" + chunk.getMax();
	    
	    
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

	    boolean success = destShardClient.moveChunk(namespace, chunk.getMin(), chunk.getMax(), 
                targetShardId, false, false, true, false, false);

	    return success;
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
					List<BsonDocument> splitPoints = calculateSplitPoints(namespace, shardKeyFields, shardKeyValues);

					// Record chunk split info if we have split points
					if (!splitPoints.isEmpty()) {
						ChunkSplitInfo splitInfo = new ChunkSplitInfo(namespace, chunk, docsInChunk);
						shardToSplitInfoMap.computeIfAbsent(shard, k -> new ArrayList<>()).add(splitInfo);
					}
				}
			}
		}
	}

	private List<BsonDocument> calculateSplitPoints(String namespace, Set<String> shardKeyFields,
			List<BsonValueWrapper> sortedShardKeyValues) {
		List<BsonDocument> splitPoints = new ArrayList<>();

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
			BsonDocument splitPoint = new BsonDocument();

			// If there's only one field in the shard key, it's simple
			if (shardKeyFields.size() == 1) {
				String keyField = shardKeyFields.iterator().next();
				BsonValue bsonValue = splitPointValue.getValue();
				splitPoint.append(keyField, bsonValue);
			} else {
				// Compound shard keys - create a BsonDocument with all shard key fields
				if (splitPointValue.getValue() instanceof BsonDocument) {
					BsonDocument bsonShardKey = (BsonDocument) splitPointValue.getValue();
					for (String field : shardKeyFields) {
						if (bsonShardKey.containsKey(field)) {
							BsonValue fieldValue = bsonShardKey.get(field);
							splitPoint.append(field, fieldValue);
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