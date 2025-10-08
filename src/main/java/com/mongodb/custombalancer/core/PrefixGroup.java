package com.mongodb.custombalancer.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;

import com.mongodb.model.Megachunk;

/**
 * Groups all chunks across multiple collections that share the same shard key prefix.
 * This is the fundamental unit for balancing to ensure MDT locality.
 */
public class PrefixGroup {

    private final ShardKeyPrefix prefix;
    private final String shard;
    private final List<Megachunk> chunks;
    private final Map<String, List<Megachunk>> chunksByNamespace;

    // Metrics
    private long estimatedDataSizeBytes;
    private long estimatedDocumentCount;
    private double operationsPerSecond;
    private boolean isWhale;

    public PrefixGroup(ShardKeyPrefix prefix, String shard) {
        this.prefix = prefix;
        this.shard = shard;
        this.chunks = new ArrayList<>();
        this.chunksByNamespace = new HashMap<>();
    }

    public void addChunk(Megachunk chunk) {
        chunks.add(chunk);

        String ns = chunk.getNs();
        chunksByNamespace.computeIfAbsent(ns, k -> new ArrayList<>()).add(chunk);

        // Validate: all chunks must be on same shard
        if (!chunk.getShard().equals(shard)) {
            throw new IllegalStateException(
                String.format("Prefix %s has chunks on multiple shards! Expected %s but found chunk on %s",
                    prefix, shard, chunk.getShard()));
        }
    }

    public ShardKeyPrefix getPrefix() {
        return prefix;
    }

    public BsonDocument getPrefixValue() {
        return prefix.getPrefixValue();
    }

    public String getShard() {
        return shard;
    }

    public List<Megachunk> getChunks() {
        return chunks;
    }

    public int getChunkCount() {
        return chunks.size();
    }

    public Map<String, List<Megachunk>> getChunksByNamespace() {
        return chunksByNamespace;
    }

    public int getNamespaceCount() {
        return chunksByNamespace.size();
    }

    public long getEstimatedDataSizeBytes() {
        return estimatedDataSizeBytes;
    }

    public void setEstimatedDataSizeBytes(long estimatedDataSizeBytes) {
        this.estimatedDataSizeBytes = estimatedDataSizeBytes;
    }

    public long getEstimatedDocumentCount() {
        return estimatedDocumentCount;
    }

    public void setEstimatedDocumentCount(long estimatedDocumentCount) {
        this.estimatedDocumentCount = estimatedDocumentCount;
    }

    public double getOperationsPerSecond() {
        return operationsPerSecond;
    }

    public void setOperationsPerSecond(double operationsPerSecond) {
        this.operationsPerSecond = operationsPerSecond;
    }

    public boolean isWhale() {
        return isWhale;
    }

    public void setWhale(boolean whale) {
        isWhale = whale;
    }

    @Override
    public String toString() {
        return String.format("PrefixGroup{prefix=%s, shard=%s, chunks=%d, namespaces=%d, dataSizeMB=%.2f, ops/sec=%.2f, whale=%s}",
                prefix, shard, chunks.size(), chunksByNamespace.size(),
                estimatedDataSizeBytes / 1024.0 / 1024.0, operationsPerSecond, isWhale);
    }
}