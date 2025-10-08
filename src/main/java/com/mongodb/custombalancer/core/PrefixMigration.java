package com.mongodb.custombalancer.core;

import java.util.List;

import org.bson.BsonDocument;

import com.mongodb.model.Megachunk;

/**
 * Represents a migration of all chunks with a specific prefix value
 * from one shard to another.
 */
public class PrefixMigration {

    private final ShardKeyPrefix prefix;
    private final String fromShard;
    private final String toShard;
    private final List<Megachunk> chunks;
    private final String strategyName;
    private final double score;  // Strategy-specific score (e.g., ops/sec, data size)

    public PrefixMigration(ShardKeyPrefix prefix, String fromShard, String toShard,
                          List<Megachunk> chunks, String strategyName, double score) {
        this.prefix = prefix;
        this.fromShard = fromShard;
        this.toShard = toShard;
        this.chunks = chunks;
        this.strategyName = strategyName;
        this.score = score;
    }

    public ShardKeyPrefix getPrefix() {
        return prefix;
    }

    public BsonDocument getPrefixValue() {
        return prefix.getPrefixValue();
    }

    public String getFromShard() {
        return fromShard;
    }

    public String getToShard() {
        return toShard;
    }

    public List<Megachunk> getChunks() {
        return chunks;
    }

    public int getChunkCount() {
        return chunks.size();
    }

    public String getStrategyName() {
        return strategyName;
    }

    public double getScore() {
        return score;
    }

    @Override
    public String toString() {
        return String.format("PrefixMigration{prefix=%s, from=%s, to=%s, chunks=%d, strategy=%s, score=%.2f}",
                prefix, fromShard, toShard, chunks.size(), strategyName, score);
    }
}