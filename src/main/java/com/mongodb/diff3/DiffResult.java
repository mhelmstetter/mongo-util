package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.Set;

public class DiffResult {
    private final long matches;
    private final long bytesProcessed;
    private final Set<BsonValue> failedKeys;
    private final Set<BsonValue> mismatchedKeys;
    private final Set<BsonValue> srcOnlyKeys;
    private final Set<BsonValue> destOnlyKeys;
    private final Namespace namespace;
    private boolean retryable = true;
    private final ChunkDef chunkDef;

    public DiffResult(long matches, long bytesProcessed, Set<BsonValue> mismatchedKeys, Set<BsonValue> srcOnlyKeys,
                      Set<BsonValue> destOnlyKeys, Namespace namespace, ChunkDef chunkDef) {
        this.matches = matches;
        this.bytesProcessed = bytesProcessed;
        this.mismatchedKeys = mismatchedKeys;
        this.srcOnlyKeys = srcOnlyKeys;
        this.destOnlyKeys = destOnlyKeys;
        this.namespace = namespace;
        this.chunkDef = chunkDef;
        this.failedKeys = new HashSet<>();
        failedKeys.addAll(this.mismatchedKeys);
        failedKeys.addAll(this.srcOnlyKeys);
        failedKeys.addAll(this.destOnlyKeys);
    }

    public long getMatches() {
        return matches;
    }

    public long getBytesProcessed() {
        return bytesProcessed;
    }

    public Set<BsonValue> getFailedKeys() {
        return failedKeys;
    }

    public Set<BsonValue> getMismatchedKeys() {
        return mismatchedKeys;
    }

    public Set<BsonValue> getSrcOnlyKeys() {
        return srcOnlyKeys;
    }

    public Set<BsonValue> getDestOnlyKeys() {
        return destOnlyKeys;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public boolean isRetryable() {
        return retryable;
    }

    public ChunkDef getChunkDef() {
        return chunkDef;
    }

    public void setRetryable(boolean retryable) {
        this.retryable = retryable;
    }
}
