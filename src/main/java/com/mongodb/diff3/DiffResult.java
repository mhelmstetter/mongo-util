package com.mongodb.diff3;

import org.bson.BsonValue;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class DiffResult {
    long matches = 0;
    long onlyOnSource = 0;
    long onlyOnDest = 0;
    long bytesProcessed = 0;
    Set<String> failedIds;

    public void addFailedKey(String id) {
        if (failedIds == null) {
            failedIds = new HashSet<>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }
}
