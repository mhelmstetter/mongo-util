package com.mongodb.mongomirror.model;

import java.util.Map;
import java.util.TreeMap;

public class MongoMirrorStatusInitialSync extends MongoMirrorStatus {
    
    public MongoMirrorStatusInitialSync(String stage, String phase, String errorMessage) {
		super(stage, phase, errorMessage);
	}

	public final static String INITIAL_SYNC = "initial sync";
    public final static String PHASE_COPYING_INDEXES = "copying indexes";
    
    private InitialSyncDetails topLevelDetails;
    
    private Map<String, InitialSyncDetails> details = new TreeMap<>();

    public Map<String, InitialSyncDetails> getDetails() {
        return details;
    }

    public void setDetails(Map<String, InitialSyncDetails> details) {
        this.details = details;
    }
    
    public long getCopiedBytes() {
        long total = 0;
        for (InitialSyncDetails d : details.values()) {
            if (d != null && d.getCopiedBytes() != null) {
                total += d.getCopiedBytes();
            }
        }
        return total;
    }
    
    public long getTotalBytes() {
        long total = 0;
        for (InitialSyncDetails d : details.values()) {
            if (d != null && d.getTotalBytes() != null) {
                total += d.getTotalBytes();
            }
        }
        return total;
    }
    
    public boolean isCopyingIndexes() {
        return this.phase.equals(PHASE_COPYING_INDEXES);
    }
    
    public double getCompletionPercent() {
        double totalBytes, copiedBytes;
        
        if (topLevelDetails != null) {
        	copiedBytes = topLevelDetails.getCopiedBytes();
        	totalBytes = topLevelDetails.getTotalBytes();
        } else {
        	totalBytes = getTotalBytes();
        	copiedBytes = getCopiedBytes();
        }
        
        if (totalBytes > 0) {
            return (copiedBytes / totalBytes) * 100.0;
        } else {
            return 0.0;
        }
        
    }

	public InitialSyncDetails getTopLevelDetails() {
		return topLevelDetails;
	}

	public void setTopLevelDetails(InitialSyncDetails topLevelDetails) {
		this.topLevelDetails = topLevelDetails;
	}

}
