package com.mongodb.diff3.shard;

import com.mongodb.diff3.DiffResult;
import org.bson.BsonValue;

import java.util.HashSet;

public class ShardDiffResult extends DiffResult {

    protected String chunkString;

    public void addFailedKey(BsonValue id) {
        if (failedIds == null) {
            failedIds = new HashSet<>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }

    @Override
    public ShardDiffResult mergeRetryResult(DiffResult rr) {
        assert rr instanceof ShardDiffResult;
        ShardDiffResult srr = (ShardDiffResult) rr;
        ShardDiffResult merged = new ShardDiffResult();
        merged.matches = srr.matches + this.matches;
        merged.onlyOnSource = srr.onlyOnSource;
        merged.onlyOnDest = srr.onlyOnDest;
        merged.failedIds = this.failedIds;
        merged.bytesProcessed = this.bytesProcessed;
        merged.namespace = srr.namespace;
        merged.chunkString = srr.chunkString;
        merged.retryable = srr.retryable;
        return merged;
    }

    @Override
    public ShardDiffResult copy() {
        ShardDiffResult copy = new ShardDiffResult();
        copy.matches = matches;
        copy.onlyOnSource = onlyOnSource;
        copy.onlyOnDest = onlyOnDest;
        copy.bytesProcessed = bytesProcessed;
        copy.namespace = namespace;
        copy.chunkString = chunkString;
        copy.retryable = retryable;
        if (failedIds == null) {
            copy.failedIds = new HashSet<>();
        } else {
            copy.failedIds = new HashSet<>(failedIds);
        }
        return copy;
    }

    @Override
    public String unitLogString() {
        return namespace.getNamespace() + "-" + chunkString;
    }

    public String toString() {
        return "DiffResult [ns=" +
                namespace.getNamespace() +
                ", matches=" +
                matches +
                ", failedIds=" +
                (failedIds == null ? 0 : failedIds.size()) +
                ", chunk=" +
                chunkString +
                "]";
    }

    public String shortString() {
        return "status=" +
                ((failedIds != null && failedIds.size() > 0) ? "FAILED" : "PASSED") +
                ", ns=" +
                namespace.getNamespace() +
                ", chunk=" +
                chunkString;
    }

    public String getChunkString() {
        return chunkString;
    }

    public void setChunkString(String chunkString) {
        this.chunkString = chunkString;
    }
}
