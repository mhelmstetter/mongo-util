package com.mongodb.mongostat;

public class MongoStatConfiguration {

    private boolean jsonOutput = false;
    private boolean includeWiredTigerStats = true;  // Default to true
    private boolean includeCollectionStats = true; // Default to true
    private boolean detailedOutput = true;          // Default to true - show everything
    private boolean includeIndexDetails = false;     // Default to false - show indexes as separate rows
    private long intervalMs = 15000;
    
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

    // Getters
    public boolean isJsonOutput() { return jsonOutput; }
    public boolean isIncludeWiredTigerStats() { return includeWiredTigerStats; }
    public boolean isIncludeCollectionStats() { return includeCollectionStats; }
    public boolean isDetailedOutput() { return detailedOutput; }
    public boolean isIncludeIndexDetails() { return includeIndexDetails; }
    public long getIntervalMs() { return intervalMs; }
}