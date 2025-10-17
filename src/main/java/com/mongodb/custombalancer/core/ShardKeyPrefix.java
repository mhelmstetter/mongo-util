package com.mongodb.custombalancer.core;

import java.util.Objects;

import org.bson.BsonDocument;
import org.bson.BsonValue;

/**
 * Represents a shard key prefix value (e.g., {customerId: 12345}).
 * This is the key component that groups chunks together to ensure
 * multi-document transactions stay on a single shard.
 */
public class ShardKeyPrefix {

    private final BsonDocument prefixValue;
    private final String[] prefixFields;

    public ShardKeyPrefix(BsonDocument prefixValue, String[] prefixFields) {
        this.prefixValue = prefixValue;
        this.prefixFields = prefixFields;
    }

    public BsonDocument getPrefixValue() {
        return prefixValue;
    }

    public String[] getPrefixFields() {
        return prefixFields;
    }

    /**
     * Extract the prefix portion from a full shard key.
     * For example, if prefix fields are [customerId] and the full key is
     * {customerId: 123, orderId: 456}, this returns {customerId: 123}.
     */
    public static BsonDocument extractPrefix(BsonDocument fullKey, String[] prefixFields) {
        BsonDocument prefix = new BsonDocument();
        for (String field : prefixFields) {
            if (fullKey.containsKey(field)) {
                prefix.append(field, fullKey.get(field));
            }
        }
        return prefix;
    }

    /**
     * Check if a chunk's min bound matches this prefix.
     */
    public boolean matches(BsonDocument chunkMin) {
        for (String field : prefixFields) {
            BsonValue prefixVal = prefixValue.get(field);
            BsonValue chunkVal = chunkMin.get(field);

            if (prefixVal == null || chunkVal == null) {
                return false;
            }

            if (!prefixVal.equals(chunkVal)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardKeyPrefix that = (ShardKeyPrefix) o;
        return Objects.equals(prefixValue, that.prefixValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prefixValue);
    }

    @Override
    public String toString() {
        return "ShardKeyPrefix{" + prefixValue.toJson() + "}";
    }
}