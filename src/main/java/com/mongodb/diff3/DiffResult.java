package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.Set;

public abstract class DiffResult {
    protected long matches;
    protected long onlyOnSource;
    protected long onlyOnDest;
    protected long bytesProcessed;
    protected Set<BsonValue> failedIds;
    protected Namespace namespace;
    protected boolean retryable = true;

    public void addFailedKey(BsonValue id) {
        if (failedIds == null) {
            failedIds = new HashSet<>();
        }
        failedIds.add(id);
    }

    public int getFailureCount() {
        return (failedIds == null) ? 0 : failedIds.size();
    }

    public abstract DiffResult mergeRetryResult(DiffResult rr);

    public abstract DiffResult copy();

    public abstract String shortString();
    public abstract String unitLogString();

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

    public Set<BsonValue> getFailedIds() {
        return failedIds;
    }

    public void setFailedIds(Set<BsonValue> failedIds) {
        this.failedIds = failedIds;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public void setNamespace(Namespace namespace) {
        this.namespace = namespace;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public void setRetryable(boolean retryable) {
        this.retryable = retryable;
    }
}
