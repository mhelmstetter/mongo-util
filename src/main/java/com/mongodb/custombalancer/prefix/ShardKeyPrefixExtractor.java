package com.mongodb.custombalancer.prefix;

import org.bson.BsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.custombalancer.core.ShardKeyPrefix;
import com.mongodb.shardsync.ShardClient;

/**
 * Extracts shard key prefix configuration from collection metadata.
 */
public class ShardKeyPrefixExtractor {

    private static final Logger logger = LoggerFactory.getLogger(ShardKeyPrefixExtractor.class);

    private final ShardClient shardClient;
    private final String[] configuredPrefixFields;

    public ShardKeyPrefixExtractor(ShardClient shardClient, String[] configuredPrefixFields) {
        this.shardClient = shardClient;
        this.configuredPrefixFields = configuredPrefixFields;
    }

    /**
     * Get the shard key pattern for a collection from config.collections.
     */
    public BsonDocument getShardKeyPattern(String namespace) {
        MongoCollection<Document> collectionsConfig = shardClient.getConfigDb().getCollection("collections");

        Document collectionDoc = collectionsConfig.find(new Document("_id", namespace)).first();
        if (collectionDoc == null) {
            logger.warn("Collection {} not found in config.collections", namespace);
            return null;
        }

        Document key = collectionDoc.get("key", Document.class);
        if (key == null) {
            logger.warn("Collection {} has no shard key", namespace);
            return null;
        }

        return BsonDocument.parse(key.toJson());
    }

    /**
     * Extract the prefix fields from the shard key based on configuration.
     * If configuredPrefixFields is null or empty, uses the first field of the shard key.
     */
    public String[] getPrefixFields(String namespace) {
        if (configuredPrefixFields != null && configuredPrefixFields.length > 0) {
            return configuredPrefixFields;
        }

        // Default: use first field of shard key
        BsonDocument shardKey = getShardKeyPattern(namespace);
        if (shardKey == null || shardKey.isEmpty()) {
            return new String[0];
        }

        String firstField = shardKey.getFirstKey();
        return new String[]{firstField};
    }

    /**
     * Extract the prefix value from a chunk's min bound.
     */
    public BsonDocument extractPrefixFromChunkMin(BsonDocument chunkMin, String[] prefixFields) {
        BsonDocument prefix = new BsonDocument();
        for (String field : prefixFields) {
            if (chunkMin.containsKey(field)) {
                prefix.append(field, chunkMin.get(field));
            }
        }
        return prefix;
    }

    /**
     * Create a ShardKeyPrefix object from a chunk's min bound.
     */
    public ShardKeyPrefix createPrefix(BsonDocument chunkMin, String[] prefixFields) {
        BsonDocument prefixValue = extractPrefixFromChunkMin(chunkMin, prefixFields);
        return new ShardKeyPrefix(prefixValue, prefixFields);
    }

    /**
     * Validate that prefix fields are consistent across all included namespaces.
     * All collections should have the same prefix fields or at least compatible ones.
     */
    public boolean validatePrefixFieldsConsistency(String[] namespaces) {
        if (namespaces == null || namespaces.length == 0) {
            return true;
        }

        String[] referencePrefixFields = null;
        for (String ns : namespaces) {
            String[] prefixFields = getPrefixFields(ns);

            if (referencePrefixFields == null) {
                referencePrefixFields = prefixFields;
                continue;
            }

            if (!arePrefixFieldsCompatible(referencePrefixFields, prefixFields)) {
                logger.warn("Inconsistent prefix fields across namespaces. Reference: {}, Current ({}): {}",
                        String.join(",", referencePrefixFields), ns, String.join(",", prefixFields));
                return false;
            }
        }

        return true;
    }

    /**
     * Check if two prefix field arrays are compatible (same fields in same order).
     */
    private boolean arePrefixFieldsCompatible(String[] fields1, String[] fields2) {
        if (fields1.length != fields2.length) {
            return false;
        }

        for (int i = 0; i < fields1.length; i++) {
            if (!fields1[i].equals(fields2[i])) {
                return false;
            }
        }

        return true;
    }
}