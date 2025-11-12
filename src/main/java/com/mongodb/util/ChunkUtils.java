package com.mongodb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonUuidUtil;

/**
 * Utility methods for working with MongoDB chunks.
 * Provides shared functionality for chunk filtering, validation, and loading
 * that can be used by both single-cluster and multi-cluster tools.
 */
public class ChunkUtils {

    private static final Logger logger = LoggerFactory.getLogger(ChunkUtils.class);

    /**
     * Load chunks for a specific shard with optional filtering.
     *
     * @param shardClient the shard client
     * @param shardId the shard identifier
     * @param excludeJumbo if true, exclude chunks marked as jumbo (unmovable)
     * @param excludeNoBalance if true, exclude chunks from collections with noBalance=true
     * @param namespaceFilter if non-null and non-empty, only include chunks from these namespaces
     * @return list of chunks matching the criteria
     */
    public static List<Megachunk> loadChunksForShard(
            ShardClient shardClient,
            String shardId,
            boolean excludeJumbo,
            boolean excludeNoBalance,
            Set<String> namespaceFilter) {

        try {
            BsonDocument query = new BsonDocument("shard", new org.bson.BsonString(shardId));
            Map<String, RawBsonDocument> chunksMap = shardClient.loadChunksCache(query);

            List<Megachunk> chunks = new ArrayList<>();
            int jumboSkipped = 0;
            int noBalanceSkipped = 0;
            int namespaceFilteredSkipped = 0;
            int configSessionsSkipped = 0;

            for (RawBsonDocument chunkDoc : chunksMap.values()) {
                // Check if chunk is jumbo
                if (excludeJumbo && isChunkJumbo(chunkDoc)) {
                    jumboSkipped++;
                    continue;
                }

                // Extract namespace
                String ns = extractNamespace(chunkDoc, shardClient);
                if (ns == null) {
                    logger.warn("Could not determine namespace for chunk, skipping");
                    continue;
                }

                // Always exclude config.system.sessions (internal chunks, not user data)
                if ("config.system.sessions".equals(ns)) {
                    configSessionsSkipped++;
                    continue;
                }

                // Apply namespace filter if provided
                if (namespaceFilter != null && !namespaceFilter.isEmpty() && !namespaceFilter.contains(ns)) {
                    namespaceFilteredSkipped++;
                    continue;
                }

                // Check if collection allows balancing
                if (excludeNoBalance && !isCollectionBalanceable(shardClient, ns)) {
                    noBalanceSkipped++;
                    continue;
                }

                // Create Megachunk from the raw document
                Megachunk chunk = new Megachunk();
                chunk.setNs(ns);
                chunk.setShard(chunkDoc.getString("shard").getValue());
                chunk.setMin(chunkDoc.getDocument("min"));

                BsonValue max = chunkDoc.get("max");
                if (max instanceof BsonDocument) {
                    chunk.setMax((BsonDocument) max);
                }

                chunks.add(chunk);
            }

            if (jumboSkipped > 0 || noBalanceSkipped > 0 || namespaceFilteredSkipped > 0 || configSessionsSkipped > 0) {
                logger.debug("Shard {}: {} chunks loaded ({} config.system.sessions, {} jumbo, {} noBalance, {} filtered)",
                           shardId, chunks.size(), configSessionsSkipped, jumboSkipped, noBalanceSkipped, namespaceFilteredSkipped);
            }

            return chunks;

        } catch (Exception e) {
            logger.error("Failed to load chunks for shard {}", shardId, e);
            return new ArrayList<>();
        }
    }

    /**
     * Extract namespace from a chunk document.
     * Handles both ns field (older) and uuid field (newer) formats.
     *
     * @param chunkDoc the chunk document
     * @param shardClient the shard client (needed for uuid → namespace lookup)
     * @return the namespace string, or null if not found
     */
    public static String extractNamespace(RawBsonDocument chunkDoc, ShardClient shardClient) {
        // Try ns field first (older format)
        if (chunkDoc.containsKey("ns")) {
            return chunkDoc.getString("ns").getValue();
        }

        // Try uuid field (newer format)
        if (chunkDoc.containsKey("uuid")) {
            org.bson.BsonBinary buuid = chunkDoc.getBinary("uuid");
            UUID uuid = BsonUuidUtil.convertBsonBinaryToUuid(buuid);
            return shardClient.getCollectionsUuidMap().get(uuid);
        }

        return null;
    }

    /**
     * Check if a chunk is marked as jumbo (unmovable due to size).
     *
     * @param chunkDoc the chunk document
     * @return true if the chunk is marked as jumbo
     */
    public static boolean isChunkJumbo(RawBsonDocument chunkDoc) {
        try {
            BsonValue jumboValue = chunkDoc.get("jumbo");
            if (jumboValue != null && jumboValue.isBoolean()) {
                return jumboValue.asBoolean().getValue();
            }
            return false;
        } catch (Exception e) {
            logger.debug("Error checking jumbo flag: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Check if a collection allows balancing.
     * Collections with noBalance=true should not be balanced.
     *
     * @param shardClient the shard client
     * @param namespace the namespace (db.collection)
     * @return true if balancing is allowed for this collection
     */
    public static boolean isCollectionBalanceable(ShardClient shardClient, String namespace) {
        try {
            Document collConfig = shardClient.getCollectionsMap().get(namespace);

            if (collConfig == null) {
                logger.debug("Collection {} not found in collections map", namespace);
                return false;
            }

            // Check noBalance flag (if present and true, balancing is disabled)
            Boolean noBalance = collConfig.getBoolean("noBalance");
            if (Boolean.TRUE.equals(noBalance)) {
                logger.debug("Collection {} has noBalance=true", namespace);
                return false;
            }

            return true;

        } catch (Exception e) {
            logger.warn("Failed to check balancing permission for {}: {}", namespace, e.getMessage());
            return false;
        }
    }

    /**
     * Check if a shard is marked as draining.
     * Draining shards should have chunks moved off of them.
     *
     * @param shardClient the shard client
     * @param shardId the shard identifier
     * @return true if the shard is draining
     */
    public static boolean isShardDraining(ShardClient shardClient, String shardId) {
        try {
            Document shardDoc = shardClient.getConfigDb()
                .getCollection("shards")
                .find(new Document("_id", shardId))
                .first();

            if (shardDoc == null) {
                return false;
            }

            Boolean draining = shardDoc.getBoolean("draining");
            return Boolean.TRUE.equals(draining);

        } catch (Exception e) {
            logger.warn("Failed to check draining status for shard {}: {}", shardId, e.getMessage());
            return false;
        }
    }

    /**
     * Check if the cluster has any zone (tag) configurations.
     *
     * @param shardClient the shard client
     * @return true if zones are configured
     */
    public static boolean hasZones(ShardClient shardClient) {
        try {
            long tagCount = shardClient.getConfigDb()
                .getCollection("tags")
                .countDocuments();
            return tagCount > 0;
        } catch (Exception e) {
            logger.error("Failed to check for zones", e);
            return false;
        }
    }

    /**
     * Get the count of chunks for a specific shard, excluding config.system.sessions.
     * Note: For MongoDB 6.0+, chunk counts are largely irrelevant due to moveRange.
     * This is provided for informational/debugging purposes only.
     *
     * @param shardClient the shard client
     * @param shardId the shard identifier
     * @return the number of chunks on the shard (excluding config.system.sessions)
     */
    public static int getChunkCount(ShardClient shardClient, String shardId) {
        try {
            // Get config.system.sessions UUID to exclude it
            Document configSessions = shardClient.getConfigDb()
                .getCollection("collections")
                .find(new Document("_id", "config.system.sessions"))
                .first();

            Document query = new Document("shard", shardId);

            // Exclude config.system.sessions by UUID (MongoDB 5.0+)
            if (configSessions != null && configSessions.containsKey("uuid")) {
                query.append("uuid", new Document("$ne", configSessions.get("uuid")));
            } else {
                // Fallback for older MongoDB versions using ns field
                query.append("ns", new Document("$ne", "config.system.sessions"));
            }

            long count = shardClient.getConfigDb()
                .getCollection("chunks")
                .countDocuments(query);

            return (int) count;
        } catch (Exception e) {
            logger.error("Failed to get chunk count for shard {}", shardId, e);
            return 0;
        }
    }
}
