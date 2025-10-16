package com.mongodb.mongostat;

public class MongoStatConfiguration {

    private boolean jsonOutput = false;
    private boolean includeWiredTigerStats = true;  // Default to true
    private boolean includeCollectionStats = true; // Default to true
    private boolean detailedOutput = true;          // Default to true - show everything
    private boolean includeIndexDetails = false;     // Default to false - show indexes as separate rows
    private long intervalMs = 15000;
    private Long manualCacheSizeBytes = null;        // Manual cache size, bypasses serverStatus
    private String sortBy = "cacheMB";               // Default sort by cacheMB descending
    private int top = 0;                             // Default 0 = show all rows
    private boolean shardPivot = false;              // Default false - normal vertical display
    private String pivotMetrics = "cacheMB,dirtyMB"; // Default metrics for pivot view

    public MongoStatConfiguration() {
    }
    
    public MongoStatConfiguration jsonOutput(boolean jsonOutput) {
        this.jsonOutput = jsonOutput;
        return this;
    }
    
    public MongoStatConfiguration includeWiredTigerStats(boolean includeWiredTigerStats) {
        this.includeWiredTigerStats = includeWiredTigerStats;
        return this;
    }
    
    public MongoStatConfiguration includeCollectionStats(boolean includeCollectionStats) {
        this.includeCollectionStats = includeCollectionStats;
        return this;
    }
    
    public MongoStatConfiguration detailedOutput(boolean detailedOutput) {
        this.detailedOutput = detailedOutput;
        return this;
    }

    public MongoStatConfiguration includeIndexDetails(boolean includeIndexDetails) {
        this.includeIndexDetails = includeIndexDetails;
        return this;
    }

    public MongoStatConfiguration intervalMs(long intervalMs) {
        this.intervalMs = intervalMs;
        return this;
    }

    public MongoStatConfiguration manualCacheSizeBytes(Long manualCacheSizeBytes) {
        this.manualCacheSizeBytes = manualCacheSizeBytes;
        return this;
    }

    public MongoStatConfiguration sortBy(String sortBy) {
        this.sortBy = sortBy;
        return this;
    }

    public MongoStatConfiguration top(int top) {
        this.top = top;
        return this;
    }

    public MongoStatConfiguration shardPivot(boolean shardPivot) {
        this.shardPivot = shardPivot;
        return this;
    }

    public MongoStatConfiguration pivotMetrics(String pivotMetrics) {
        this.pivotMetrics = pivotMetrics;
        return this;
    }

    // Getters
    public boolean isJsonOutput() { return jsonOutput; }
    public boolean isIncludeWiredTigerStats() { return includeWiredTigerStats; }
    public boolean isIncludeCollectionStats() { return includeCollectionStats; }
    public boolean isDetailedOutput() { return detailedOutput; }
    public boolean isIncludeIndexDetails() { return includeIndexDetails; }
    public long getIntervalMs() { return intervalMs; }
    public Long getManualCacheSizeBytes() { return manualCacheSizeBytes; }
    public String getSortBy() { return sortBy; }
    public int getTop() { return top; }
    public boolean isShardPivot() { return shardPivot; }
    public String getPivotMetrics() { return pivotMetrics; }
}