package com.mongodb.custombalancer.prefix;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.custombalancer.CustomBalancerConfig;
import com.mongodb.custombalancer.core.PrefixGroup;
import com.mongodb.custombalancer.core.ShardKeyPrefix;
import com.mongodb.model.Megachunk;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.util.bson.BsonValueWrapper;

/**
 * Groups chunks by their shard key prefix to enable prefix-aware balancing.
 */
public class PrefixChunkGrouper {

    private static final Logger logger = LoggerFactory.getLogger(PrefixChunkGrouper.class);

    private final CustomBalancerConfig config;
    private final ChunkManager chunkManager;
    private final ShardKeyPrefixExtractor prefixExtractor;

    public PrefixChunkGrouper(CustomBalancerConfig config, ChunkManager chunkManager,
                             ShardKeyPrefixExtractor prefixExtractor) {
        this.config = config;
        this.chunkManager = chunkManager;
        this.prefixExtractor = prefixExtractor;
    }

    /**
     * Load all chunks and group them by prefix value.
     * Returns a map of prefix value -> PrefixGroup.
     */
    public Map<BsonDocument, PrefixGroup> loadAndGroupChunks() {
        logger.info("Loading and grouping chunks by shard key prefix");

        // Load chunks using ChunkManager
        Map<String, RawBsonDocument> sourceChunksCache = new LinkedHashMap<>();
        Map<String, NavigableMap<BsonValueWrapper, Megachunk>> chunkMap = new HashMap<>();

        chunkManager.loadChunkMap(null, sourceChunksCache, (Map) chunkMap);
        config.setChunkMap((Map) chunkMap);

        logger.info("Loaded {} chunks across {} namespaces", sourceChunksCache.size(), chunkMap.size());

        // Group chunks by prefix
        Map<BsonDocument, PrefixGroup> prefixGroups = new HashMap<>();

        for (Map.Entry<String, NavigableMap<BsonValueWrapper, Megachunk>> entry : chunkMap.entrySet()) {
            String namespace = entry.getKey();
            NavigableMap<BsonValueWrapper, Megachunk> chunks = entry.getValue();

            String[] prefixFields = prefixExtractor.getPrefixFields(namespace);
            if (prefixFields == null || prefixFields.length == 0) {
                logger.warn("Skipping namespace {} - could not determine prefix fields", namespace);
                continue;
            }

            logger.debug("Processing namespace {} with prefix fields: {}", namespace, String.join(",", prefixFields));

            for (Megachunk chunk : chunks.values()) {
                BsonDocument chunkMin = chunk.getMin();
                if (chunkMin == null) {
                    logger.warn("Chunk has null min bound: {}", chunk);
                    continue;
                }

                // Extract prefix from chunk min
                BsonDocument prefixValue = prefixExtractor.extractPrefixFromChunkMin(chunkMin, prefixFields);
                ShardKeyPrefix prefix = new ShardKeyPrefix(prefixValue, prefixFields);

                // Get or create PrefixGroup
                PrefixGroup group = prefixGroups.computeIfAbsent(prefixValue,
                        k -> new PrefixGroup(prefix, chunk.getShard()));

                // Validate shard consistency
                if (!group.getShard().equals(chunk.getShard())) {
                    logger.error("CONSISTENCY ERROR: Prefix {} has chunks on multiple shards! " +
                            "Group shard: {}, Chunk shard: {}, Namespace: {}",
                            prefixValue, group.getShard(), chunk.getShard(), namespace);
                    throw new IllegalStateException(
                            String.format("Prefix %s spans multiple shards: %s and %s. " +
                                    "This violates the assumption that presplit script co-located all chunks.",
                                    prefixValue, group.getShard(), chunk.getShard()));
                }

                group.addChunk(chunk);
            }
        }

        logger.info("Grouped {} chunks into {} prefix groups", sourceChunksCache.size(), prefixGroups.size());

        // Log distribution summary
        logPrefixDistributionSummary(prefixGroups);

        return prefixGroups;
    }

    /**
     * Log summary of prefix distribution across shards.
     */
    private void logPrefixDistributionSummary(Map<BsonDocument, PrefixGroup> prefixGroups) {
        Map<String, Integer> prefixCountByShard = new HashMap<>();
        Map<String, Integer> chunkCountByShard = new HashMap<>();

        for (PrefixGroup group : prefixGroups.values()) {
            String shard = group.getShard();
            prefixCountByShard.merge(shard, 1, Integer::sum);
            chunkCountByShard.merge(shard, group.getChunkCount(), Integer::sum);
        }

        logger.info("Prefix distribution by shard:");
        for (String shard : prefixCountByShard.keySet()) {
            logger.info("  {}: {} prefixes, {} chunks",
                    shard, prefixCountByShard.get(shard), chunkCountByShard.get(shard));
        }
    }

    /**
     * Reload chunks for a specific namespace (useful after splits).
     */
    public void reloadNamespace(String namespace, Map<BsonDocument, PrefixGroup> existingGroups) {
        logger.info("Reloading chunks for namespace: {}", namespace);

        Map<String, RawBsonDocument> sourceChunksCache = new LinkedHashMap<>();
        Map<String, NavigableMap<BsonValueWrapper, Megachunk>> chunkMap = new HashMap<>();

        chunkManager.loadChunkMap(namespace, sourceChunksCache, (Map) chunkMap);

        // Remove old chunks for this namespace from prefix groups
        for (PrefixGroup group : existingGroups.values()) {
            group.getChunksByNamespace().remove(namespace);
        }

        // Re-add updated chunks
        String[] prefixFields = prefixExtractor.getPrefixFields(namespace);
        if (prefixFields == null || prefixFields.length == 0) {
            logger.warn("Could not determine prefix fields for {}", namespace);
            return;
        }

        NavigableMap<BsonValueWrapper, Megachunk> chunks = chunkMap.get(namespace);
        if (chunks == null) {
            logger.warn("No chunks found for namespace: {}", namespace);
            return;
        }

        for (Megachunk chunk : chunks.values()) {
            BsonDocument prefixValue = prefixExtractor.extractPrefixFromChunkMin(chunk.getMin(), prefixFields);
            ShardKeyPrefix prefix = new ShardKeyPrefix(prefixValue, prefixFields);

            PrefixGroup group = existingGroups.computeIfAbsent(prefixValue,
                    k -> new PrefixGroup(prefix, chunk.getShard()));

            group.addChunk(chunk);
        }

        logger.info("Reloaded {} chunks for namespace {}", chunks.size(), namespace);
    }
}