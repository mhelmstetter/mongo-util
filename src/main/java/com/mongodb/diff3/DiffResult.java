package com.mongodb.diff3;

import org.bson.BsonValue;

import java.util.LinkedList;
import java.util.List;

public class DiffResult {
    long matches = 0;
    long onlyOnSource = 0;
    long onlyOnDest = 0;
    long bytesProcessed = 0;
    List<BsonValue> failedIds;

    public void addFailedKey(BsonValue id) {
        if (failedIds == null) {
            failedIds = new LinkedList<BsonValue>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }
}
