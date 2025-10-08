package com.mongodb.custombalancer.core;

/**
 * Unified view of shard load metrics including data size, activity, and chunk count.
 */
public class ShardLoadMetrics {

    private final String shardId;

    private long dataSizeBytes;
    private long documentCount;
    private int chunkCount;
    private int prefixCount;

    private double readOpsPerSecond;
    private double writeOpsPerSecond;
    private double totalOpsPerSecond;

    public ShardLoadMetrics(String shardId) {
        this.shardId = shardId;
    }

    public String getShardId() {
        return shardId;
    }

    public long getDataSizeBytes() {
        return dataSizeBytes;
    }

    public void setDataSizeBytes(long dataSizeBytes) {
        this.dataSizeBytes = dataSizeBytes;
    }

    public long getDataSizeMB() {
        return dataSizeBytes / 1024 / 1024;
    }

    public long getDocumentCount() {
        return documentCount;
    }

    public void setDocumentCount(long documentCount) {
        this.documentCount = documentCount;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public void setChunkCount(int chunkCount) {
        this.chunkCount = chunkCount;
    }

    public int getPrefixCount() {
        return prefixCount;
    }

    public void setPrefixCount(int prefixCount) {
        this.prefixCount = prefixCount;
    }

    public double getReadOpsPerSecond() {
        return readOpsPerSecond;
    }

    public void setReadOpsPerSecond(double readOpsPerSecond) {
        this.readOpsPerSecond = readOpsPerSecond;
    }

    public double getWriteOpsPerSecond() {
        return writeOpsPerSecond;
    }

    public void setWriteOpsPerSecond(double writeOpsPerSecond) {
        this.writeOpsPerSecond = writeOpsPerSecond;
    }

    public double getTotalOpsPerSecond() {
        return totalOpsPerSecond;
    }

    public void setTotalOpsPerSecond(double totalOpsPerSecond) {
        this.totalOpsPerSecond = totalOpsPerSecond;
    }

    public void addDataSize(long bytes) {
        this.dataSizeBytes += bytes;
    }

    public void addDocumentCount(long count) {
        this.documentCount += count;
    }

    public void addChunkCount(int count) {
        this.chunkCount += count;
    }

    public void addPrefixCount(int count) {
        this.prefixCount += count;
    }

    public void addOperations(double readOps, double writeOps) {
        this.readOpsPerSecond += readOps;
        this.writeOpsPerSecond += writeOps;
        this.totalOpsPerSecond += (readOps + writeOps);
    }

    @Override
    public String toString() {
        return String.format("ShardLoadMetrics{shard=%s, dataSizeMB=%,d, docs=%,d, chunks=%d, prefixes=%d, " +
                        "readOps/s=%.2f, writeOps/s=%.2f, totalOps/s=%.2f}",
                shardId, getDataSizeMB(), documentCount, chunkCount, prefixCount,
                readOpsPerSecond, writeOpsPerSecond, totalOpsPerSecond);
    }
}