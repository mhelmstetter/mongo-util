package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.Set;

public abstract class DiffResult {
    protected long matches;
    protected long bytesProcessed;
    protected Set<BsonValue> failedKeys = new HashSet<>();
    protected Set<BsonValue> keysOnlyOnSource = new HashSet<>();
    protected Set<BsonValue> keysOnlyOnDest = new HashSet<>();
    protected Namespace namespace;
    protected boolean retryable = true;

    public void addFailedKey(BsonValue id) {
        failedKeys.add(id);
    }
    
    public void addOnlyOnSourceKeys(Set<BsonValue> keys) {
        keysOnlyOnSource.addAll(keys);
    }
    
    public void addOnlyOnDestKeys(Set<BsonValue> keys) {
        keysOnlyOnDest.addAll(keys);
    }

    public int getFailureCount() {
        return failedKeys.size();
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

    public long getOnlyOnSourceCount() {
        return keysOnlyOnSource.size();
    }

    public long getOnlyOnDestCount() {
        return keysOnlyOnDest.size();
    }

    public long getBytesProcessed() {
        return bytesProcessed;
    }

    public void setBytesProcessed(long bytesProcessed) {
        this.bytesProcessed = bytesProcessed;
    }

    public Set<BsonValue> getFailedKeys() {
        return failedKeys;
    }

    public void setFailedIds(Set<BsonValue> failedIds) {
        this.failedKeys = failedIds;
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

	public Set<BsonValue> getKeysOnlyOnSource() {
		return keysOnlyOnSource;
	}

	public Set<BsonValue> getKeysOnlyOnDest() {
		return keysOnlyOnDest;
	}
}
