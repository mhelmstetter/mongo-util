package com.mongodb.diff3;

import java.util.HashSet;
import java.util.Set;

public class DiffResult {
    protected long matches = 0;
    protected long onlyOnSource = 0;
    protected long onlyOnDest = 0;
    protected long bytesProcessed = 0;
    protected Set<String> failedIds;
    protected String ns;
    protected String chunkString;
    protected boolean retryable = true;

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
        merged.matches = rr.matches + this.matches;
        merged.onlyOnSource = rr.onlyOnSource;
        merged.onlyOnDest = rr.onlyOnDest;
        merged.failedIds = this.failedIds;
        merged.bytesProcessed = this.bytesProcessed;
        merged.ns = rr.ns;
        merged.chunkString = rr.chunkString;
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
        if (failedIds == null) {
            copy.failedIds = new HashSet<>();
        } else {
            copy.failedIds = new HashSet<>(failedIds);
        }
        return copy;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiffResult [ns=");
        builder.append(ns);
        builder.append(", matches=");
        builder.append(matches);
        builder.append(", failedIds=");
        builder.append(failedIds == null ? 0 : failedIds.size());
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
