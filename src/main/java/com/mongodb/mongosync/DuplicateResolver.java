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
	    logger.info("Starting iterative conflict resolution process");
	    
	    // Process each namespace with duplicates
	    for (String namespace : duplicateIdToDocsMap.keySet()) {
	        logger.info("Processing namespace: {}", namespace);
	        
	        // Get shard key information
	        Document collMeta = collectionsMap.get(namespace);
	        if (collMeta == null) {
	            logger.error("No collection metadata found for namespace: {}", namespace);
	            continue;
	        }
	        
	        Document shardKeyDoc = (Document)collMeta.get("key");
	        Set<String> shardKeyFields = shardKeyDoc.keySet();
	        String primaryShardKey = shardKeyFields.iterator().next();
	        
	        // Set of _ids that have been successfully resolved
	        Set<Object> resolvedIds = new HashSet<>();
	        // Maximum number of iterations to try
	        final int MAX_ITERATIONS = 5;
	        
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
	                if (docsWithSameId.size() <= 1 || resolvedIds.contains(id)) continue;
	                
	                // For each document with this _id
	                for (Document doc : docsWithSameId) {
	                    // Get shard key value
	                    Object shardKeyValue = doc.get(primaryShardKey);
	                    if (shardKeyValue == null) continue;
	                    
	                    // Find which chunk this document belongs to
	                    try {
	                        BsonValueWrapper shardKeyWrapper = getShardKeyWrapper(shardKeyFields, doc);
	                        NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	                        if (chunkMap == null) continue;
	                        
	                        Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyWrapper);
	                        if (chunkEntry == null) continue;
	                        
	                        CountingMegachunk chunk = chunkEntry.getValue();
	                        docToChunkMap.put(doc, chunk);
	                    } catch (Exception e) {
	                        logger.error("Error finding chunk for doc with _id {}: {}", id, e.getMessage());
	                    }
	                }
	            }
	            
	            // Analyze which _ids are in conflict (on same shard)
	            Map<Object, Map<String, List<Document>>> conflictingIds = new HashMap<>();
	            
	            for (Map.Entry<Object, List<Document>> entry : duplicatesMap.entrySet()) {
	                Object id = entry.getKey();
	                List<Document> docsWithSameId = entry.getValue();
	                
	                // Skip if not a duplicate or already resolved
	                if (docsWithSameId.size() <= 1 || resolvedIds.contains(id)) continue;
	                
	                // Group documents by shard
	                Map<String, List<Document>> shardToDocsMap = new HashMap<>();
	                
	                for (Document doc : docsWithSameId) {
	                    CountingMegachunk chunk = docToChunkMap.get(doc);
	                    if (chunk == null) continue;
	                    
	                    String shardId = chunk.getShard();
	                    shardToDocsMap.computeIfAbsent(shardId, k -> new ArrayList<>()).add(doc);
	                }
	                
	                // If all docs are on the same shard, we have a conflict
	                if (shardToDocsMap.size() == 1) {
	                    conflictingIds.put(id, shardToDocsMap);
	                    
	                    // Log the conflict
	                    String shard = shardToDocsMap.keySet().iterator().next();
	                    logger.info("Conflict in iteration {}: _id {} has {} documents on shard {}",
	                              iteration, id, shardToDocsMap.get(shard).size(), shard);
	                    
	                    // Log the documents
	                    for (Document doc : shardToDocsMap.get(shard)) {
	                        CountingMegachunk chunk = docToChunkMap.get(doc);
	                        Object shardKeyValue = doc.get(primaryShardKey);
	                        
	                        logger.info("  Doc with _id {}, shardKey: {}, in chunk with bounds: min={}, max={}",
	                                  id, shardKeyValue, chunk.getMin(), chunk.getMax());
	                    }
	                } else {
	                    // This _id is resolved
	                    resolvedIds.add(id);
	                    logger.info("Already resolved in iteration {}: _id {} is on shards: {}",
	                              iteration, id, shardToDocsMap.keySet());
	                }
	            }
	            
	            // If no conflicts left, we're done
	            if (conflictingIds.isEmpty()) {
	                logger.info("No conflicts remaining after iteration {}, breaking early", iteration);
	                break;
	            }
	            
	            logger.info("Found {} conflicting _ids in iteration {}", conflictingIds.size(), iteration);
	            
	            // Available shards
	            Set<String> allShards = new HashSet<>();
	            allShards.add("shA");
	            allShards.add("shard_B");
	            
	            // Process conflicts by prioritizing:
	            // 1. Conflicts that involve fewer other _ids when moved
	            // 2. Conflicts where docs are in different chunks
	            List<Object> sortedIds = new ArrayList<>(conflictingIds.keySet());
	            
	            // Sort by complexity (move simpler conflicts first)
	            Collections.sort(sortedIds, (id1, id2) -> {
	                Document doc1 = conflictingIds.get(id1).values().iterator().next().get(0);
	                Document doc2 = conflictingIds.get(id2).values().iterator().next().get(0);
	                
	                CountingMegachunk chunk1 = docToChunkMap.get(doc1);
	                CountingMegachunk chunk2 = docToChunkMap.get(doc2);
	                
	                // Count how many other _ids would be affected by moving each chunk
	                int affected1 = countAffectedIds(chunk1, docToChunkMap, id1);
	                int affected2 = countAffectedIds(chunk2, docToChunkMap, id2);
	                
	                // Sort by fewer affected ids
	                return Integer.compare(affected1, affected2);
	            });
	            
	            // Track which chunks have been moved in this iteration
	            Set<Integer> movedChunks = new HashSet<>();
	            
	            // Resolve conflicts
	            for (Object id : sortedIds) {
	                Map<String, List<Document>> shardToDocsMap = conflictingIds.get(id);
	                String currentShard = shardToDocsMap.keySet().iterator().next();
	                List<Document> docsOnShard = shardToDocsMap.get(currentShard);
	                
	                // Need at least 2 docs to have a conflict
	                if (docsOnShard.size() < 2) continue;
	                
	                // Find target shard
	                String targetShard = null;
	                for (String shard : allShards) {
	                    if (!shard.equals(currentShard)) {
	                        targetShard = shard;
	                        break;
	                    }
	                }
	                
	                if (targetShard == null) {
	                    logger.error("Could not find a target shard different from {}", currentShard);
	                    continue;
	                }
	                
	                // Get chunks for these documents
	                List<CountingMegachunk> chunksForDocs = new ArrayList<>();
	                for (Document doc : docsOnShard) {
	                    chunksForDocs.add(docToChunkMap.get(doc));
	                }
	                
	                // Ensure we have different chunks
	                if (new HashSet<>(chunksForDocs).size() < 2) {
	                    logger.warn("Docs with _id {} are all in the same chunk, can't separate", id);
	                    continue;
	                }
	                
	                // Choose which chunk to move - prefer the one that affects fewer other _ids
	                CountingMegachunk bestChunkToMove = null;
	                int minAffectedIds = Integer.MAX_VALUE;
	                
	                for (Document doc : docsOnShard) {
	                    CountingMegachunk chunk = docToChunkMap.get(doc);
	                    
	                    // Skip if this chunk was already moved in this iteration
	                    if (movedChunks.contains(chunk.hashCode())) continue;
	                    
	                    // Count how many other _ids would be affected
	                    int affectedIds = countAffectedIds(chunk, docToChunkMap, id);
	                    
	                    // Check if this move would create new conflicts for already resolved IDs
	                    boolean wouldCreateNewConflicts = false;
	                    for (Object resolvedId : resolvedIds) {
	                        if (wouldCreateConflictForId(resolvedId, chunk, docToChunkMap, targetShard)) {
	                            wouldCreateNewConflicts = true;
	                            break;
	                        }
	                    }
	                    
	                    // Skip if it would create new conflicts
	                    if (wouldCreateNewConflicts) {
	                        logger.info("Skipping move of chunk for _id {} as it would create new conflicts", id);
	                        continue;
	                    }
	                    
	                    // Choose this chunk if it affects fewer IDs
	                    if (affectedIds < minAffectedIds) {
	                        bestChunkToMove = chunk;
	                        minAffectedIds = affectedIds;
	                    }
	                }
	                
	                // If we found a chunk to move
	                if (bestChunkToMove != null) {
	                    // Log affected IDs
	                    if (minAffectedIds > 0) {
	                        logger.info("Moving chunk will also affect {} other _ids", minAffectedIds);
	                    }
	                    
	                    // Move the chunk
	                    logger.info("Moving chunk with bounds min: {}, max: {} from shard {} to shard {} for _id {}",
	                              bestChunkToMove.getMin(), bestChunkToMove.getMax(), 
	                              currentShard, targetShard, id);
	                    
	                    boolean success = moveChunkWithRetry(namespace, bestChunkToMove, targetShard);
	                    
	                    if (success) {
	                        logger.info("Successfully moved chunk to shard {}", targetShard);
	                        
	                        // Update our records
	                        bestChunkToMove.setShard(targetShard);
	                        
	                        // Update all documents in this chunk
	                        for (Map.Entry<Document, CountingMegachunk> entry : docToChunkMap.entrySet()) {
	                            if (entry.getValue().equals(bestChunkToMove)) {
	                                entry.setValue(bestChunkToMove);
	                            }
	                        }
	                        
	                        // Mark this chunk as moved
	                        movedChunks.add(bestChunkToMove.hashCode());
	                        
	                        // Mark this _id as resolved
	                        resolvedIds.add(id);
	                    } else {
	                        logger.error("Failed to move chunk for _id {}", id);
	                    }
	                } else {
	                    logger.warn("Could not find a suitable chunk to move for _id {}", id);
	                }
	            }
	            
	            // Refresh chunk cache for next iteration
	            refreshChunkCache(namespace);
	            
	            // Check if we made progress
	            if (movedChunks.isEmpty()) {
	                logger.info("No progress made in iteration {}, breaking early", iteration);
	                break;
	            }
	        }
	        
	        // Final verification
	        verifyDuplicateResolution(namespace, shardKeyFields);
	    }
	}

	// Count how many _ids would be affected by moving this chunk
	private int countAffectedIds(CountingMegachunk chunk, 
	                           Map<Document, CountingMegachunk> docToChunkMap,
	                           Object excludeId) {
	    Set<Object> affectedIds = new HashSet<>();
	    
	    for (Map.Entry<Document, CountingMegachunk> entry : docToChunkMap.entrySet()) {
	        Document doc = entry.getKey();
	        CountingMegachunk docChunk = entry.getValue();
	        Object docId = doc.get("_id");
	        
	        // If this doc is in the chunk and it's not our target id
	        if (docChunk.equals(chunk) && !docId.equals(excludeId)) {
	            affectedIds.add(docId);
	        }
	    }
	    
	    return affectedIds.size();
	}

	// Check if moving this chunk would create a new conflict for a resolved ID
	private boolean wouldCreateConflictForId(Object id, 
	                                      CountingMegachunk chunkToMove,
	                                      Map<Document, CountingMegachunk> docToChunkMap,
	                                      String targetShard) {
	    // Find all docs with this id
	    List<Document> docsWithId = new ArrayList<>();
	    List<CountingMegachunk> chunksWithId = new ArrayList<>();
	    
	    for (Map.Entry<Document, CountingMegachunk> entry : docToChunkMap.entrySet()) {
	        Document doc = entry.getKey();
	        if (doc.get("_id").equals(id)) {
	            docsWithId.add(doc);
	            chunksWithId.add(entry.getValue());
	        }
	    }
	    
	    // Check if any of these docs are in the chunk we want to move
	    boolean docInMovingChunk = false;
	    for (CountingMegachunk chunk : chunksWithId) {
	        if (chunk.equals(chunkToMove)) {
	            docInMovingChunk = true;
	            break;
	        }
	    }
	    
	    // If none of the docs for this ID are in the moving chunk, no conflict
	    if (!docInMovingChunk) {
	        return false;
	    }
	    
	    // Check if any other docs with this ID are already on the target shard
	    for (CountingMegachunk chunk : chunksWithId) {
	        if (!chunk.equals(chunkToMove) && chunk.getShard().equals(targetShard)) {
	            // Found a doc already on target shard, moving would create conflict
	            return true;
	        }
	    }
	    
	    return false;
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
	        if (docsWithSameId.size() <= 1) continue;
	        
	        // Find which shard each document is on
	        Map<String, List<Document>> shardToDocsMap = new HashMap<>();
	        
	        for (Document doc : docsWithSameId) {
	            BsonValueWrapper shardKeyValue = getShardKeyWrapper(shardKeyFields, doc);
	            
	            NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(namespace);
	            if (chunkMap == null) continue;
	            
	            Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = chunkMap.floorEntry(shardKeyValue);
	            if (chunkEntry == null) continue;
	            
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
	                    if (details.length() > 0) details.append(", ");
	                    details.append(primaryShardKey).append(": ").append(doc.get(primaryShardKey));
	                }
	                
	                logger.warn("REMAINING CONFLICT: _id {} has {} documents on shard {}: {}", 
	                          id, docsOnShard.size(), shard, details);
	                
	                remainingConflicts++;
	            }
	        } else {
	            // Log successful resolution
	            logger.info("Successfully resolved conflict for _id {}, documents are on shards: {}", 
	                      id, shardToDocsMap.keySet());
	        }
	    }
	    
	    if (remainingConflicts > 0) {
	        logger.warn("Found {} remaining _id conflicts after migrations for namespace {}", 
	                  remainingConflicts, namespace);
	    } else {
	        logger.info("Successfully resolved all conflicts for namespace {}", namespace);
	    }
	}

	/**
	 * Find cycles in a dependency graph
	 * 
	 * @param graph Map representing the graph where key is node and value is set of nodes it points to
	 * @return List of cycles found in the graph
	 */
	private List<List<Object>> findCycles(Map<Object, Set<Object>> graph) {
	    List<List<Object>> cycles = new ArrayList<>();
	    Set<Object> visited = new HashSet<>();
	    Set<Object> onStack = new HashSet<>();
	    Map<Object, Object> edgeTo = new HashMap<>();
	    
	    // For each node in the graph, if not visited, start DFS from it
	    for (Object node : graph.keySet()) {
	        if (!visited.contains(node)) {
	            dfs(graph, node, visited, onStack, edgeTo, cycles);
	        }
	    }
	    
	    return cycles;
	}

	/**
	 * Depth-first search to find cycles
	 */
	private void dfs(Map<Object, Set<Object>> graph, Object node, Set<Object> visited, 
	                Set<Object> onStack, Map<Object, Object> edgeTo, List<List<Object>> cycles) {
	    visited.add(node);
	    onStack.add(node);
	    
	    // Visit all neighbors
	    Set<Object> neighbors = graph.getOrDefault(node, Collections.emptySet());
	    for (Object neighbor : neighbors) {
	        // If we haven't visited this neighbor, visit it
	        if (!visited.contains(neighbor)) {
	            edgeTo.put(neighbor, node);
	            dfs(graph, neighbor, visited, onStack, edgeTo, cycles);
	        } 
	        // If neighbor is on stack, we found a cycle
	        else if (onStack.contains(neighbor)) {
	            List<Object> cycle = new ArrayList<>();
	            for (Object x = node; !x.equals(neighbor); x = edgeTo.get(x)) {
	                cycle.add(x);
	            }
	            cycle.add(neighbor);
	            cycle.add(node);
	            Collections.reverse(cycle);
	            cycles.add(cycle);
	        }
	    }
	    
	    onStack.remove(node);
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
	        logger.debug("Not enough shard key values to create split points for namespace: {}", namespace);
	        return splitPoints;
	    }

	    // Log the range of values we're working with for debugging
	    logger.debug("Calculating split points for namespace: {} with shard key values range: min={}, max={}", 
	             namespace, sortedShardKeyValues.get(0), sortedShardKeyValues.get(sortedShardKeyValues.size()-1));

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
	                logger.debug("Generated split point at index {}: key={}, value={} (int32)", 
	                    i, keyField, keyValue);
	            } else if (bsonValue.isInt64()) {
	                keyValue = bsonValue.asInt64().getValue();
	                logger.debug("Generated split point at index {}: key={}, value={} (int64)", 
	                    i, keyField, keyValue);
	            } else if (bsonValue.isDouble()) {
	                keyValue = bsonValue.asDouble().getValue();
	                logger.debug("Generated split point at index {}: key={}, value={} (double)", 
	                    i, keyField, keyValue);
	            } else if (bsonValue.isString()) {
	                keyValue = bsonValue.asString().getValue();
	                logger.debug("Generated split point at index {}: key={}, value={} (string)", 
	                    i, keyField, keyValue);
	            } else if (bsonValue.isObjectId()) {
	                keyValue = bsonValue.asObjectId().getValue();
	                logger.debug("Generated split point at index {}: key={}, value={} (objectId)", 
	                    i, keyField, keyValue);
	            } else if (bsonValue.isBoolean()) {
	                keyValue = bsonValue.asBoolean().getValue();
	                logger.debug("Generated split point at index {}: key={}, value={} (boolean)", 
	                    i, keyField, keyValue);
	            } else {
	                // For other types, use a safe conversion
	                keyValue = BsonValueConverter.convertBsonValueToObject(bsonValue);
	                logger.debug("Generated split point at index {}: key={}, value={} (converted)", 
	                    i, keyField, keyValue);
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
	                logger.debug("Generated compound key split point at index {}: fields={}, values={}",
	                        i, shardKeyFields, splitPoint);
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