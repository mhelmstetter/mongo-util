package com.mongodb.mongosync.model;

public enum MongoSyncState {
	
    IDLE("mongosync is initialized and ready for a sync job to begin."),
    RUNNING("The sync process is currently running. Data is synced to the destination cluster, " 
    		+ "and subsequent writes to the source cluster are applied to the destination cluster."),
    PAUSED("The sync process is paused. To resume, send a request to the /resume endpoint."),
    COMMITTING("The cutover for the sync process has started. Transitioning to the COMMITTED phase depends on lagTimeSeconds."),
    COMMITTED("The cutover is complete."),
    REVERSING("The sync process is reversing. Metadata is copied from the destination to the source, and MongoDB swaps the source and destination clusters."),
	FAILED("The process has failed");
	
    private final String description;

    MongoSyncState(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }
}

