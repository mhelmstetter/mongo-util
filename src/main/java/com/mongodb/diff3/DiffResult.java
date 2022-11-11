package com.mongodb.diff3;

import org.bson.BsonValue;

import java.util.*;

public class DiffResult {
    private long matches = 0;
    private long onlyOnSource = 0;
    private long onlyOnDest = 0;
    private long bytesProcessed = 0;
    private Set<String> failedIds;
    private String ns;
    private String chunkString;
    private boolean retryable = true;

    public void addFailedKey(String id) {
        if (failedIds == null) {
            failedIds = new HashSet<>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }

    public DiffResult mergeRetryResult(DiffResult rr) {
        DiffResult merged = new DiffResult();
        merged.matches = rr.matches;// + this.matches;
        merged.onlyOnSource = rr.onlyOnSource;
        merged.onlyOnDest = rr.onlyOnDest;
        merged.failedIds = rr.failedIds;
        merged.bytesProcessed = this.bytesProcessed;
        merged.ns = this.ns;
        merged.chunkString = this.chunkString;
        merged.retryable = rr.retryable;
        return merged;
    }

    public DiffResult copy() {
        DiffResult copy = new DiffResult();
        copy.matches = matches;
        copy.onlyOnSource = onlyOnSource;
        copy.onlyOnDest = onlyOnDest;
        copy.bytesProcessed = bytesProcessed;
        copy.ns = ns;
        copy.chunkString = chunkString;
        copy.retryable = retryable;
        copy.failedIds = new HashSet<>(failedIds);
        return copy;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiffResult [ns=");
        builder.append(ns);
        builder.append(", matches=");
        builder.append(matches);
        builder.append(", failedIds=");
        builder.append(failedIds.size());
        builder.append(", chunk=");
        builder.append(chunkString);
        builder.append("]");
        return builder.toString();
    }

    public String shortString() {
        StringBuilder sb = new StringBuilder();
        sb.append("status=");
        sb.append((failedIds != null && failedIds.size() > 0) ? "FAILED" : "PASSED");
        sb.append(", ns=");
        sb.append(ns);
        sb.append(", chunk=");
        sb.append(chunkString);
        return sb.toString();
    }

    public long getMatches() {
        return matches;
    }

    public void setMatches(long matches) {
        this.matches = matches;
    }

    public long getOnlyOnSource() {
        return onlyOnSource;
    }

    public void setOnlyOnSource(long onlyOnSource) {
        this.onlyOnSource = onlyOnSource;
    }

    public long getOnlyOnDest() {
        return onlyOnDest;
    }

    public void setOnlyOnDest(long onlyOnDest) {
        this.onlyOnDest = onlyOnDest;
    }

    public long getBytesProcessed() {
        return bytesProcessed;
    }

    public void setBytesProcessed(long bytesProcessed) {
        this.bytesProcessed = bytesProcessed;
    }

    public Set<String> getFailedIds() {
        return failedIds;
    }

    public void setFailedIds(Set<String> failedIds) {
        this.failedIds = failedIds;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public String getChunkString() {
        return chunkString;
    }

    public void setChunkString(String chunkString) {
        this.chunkString = chunkString;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public void setRetryable(boolean retryable) {
        this.retryable = retryable;
    }
}
