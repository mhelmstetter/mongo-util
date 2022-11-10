package com.mongodb.diff3;

import org.bson.BsonValue;

import java.util.LinkedList;
import java.util.List;

public class DiffResult {
    long matches = 0;
    long onlyOnSource = 0;
    long onlyOnDest = 0;
    long bytesProcessed = 0;
    List<String> failedIds;

    public void addFailedKey(String id) {
        if (failedIds == null) {
            failedIds = new LinkedList<>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }

    public DiffResult mergeRetryResult(DiffResult rr) {
        matches += rr.matches;
        onlyOnSource = rr.onlyOnSource;
        onlyOnDest = rr.onlyOnDest;
        failedIds = rr.failedIds;
        return this;
    }
}
