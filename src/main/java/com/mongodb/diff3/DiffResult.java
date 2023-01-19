package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonValue;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class DiffResult {
    private final long matches;
    private final long bytesProcessed;
    private final Set<BsonValue> failedKeys;
    private final Set<MismatchEntry> mismatchedEntries;
    private final Set<BsonValue> srcOnlyKeys;
    private final Set<BsonValue> destOnlyKeys;
    private final Namespace namespace;
    private boolean retryable = true;
    private final ChunkDef chunkDef;

    static class MismatchEntry {
        private final BsonValue key;
        private final String srcChksum;
        private final String destChksum;

        MismatchEntry(BsonValue key, String srcChksum, String destChksum) {
            this.key = key;
            this.srcChksum = srcChksum;
            this.destChksum = destChksum;
        }

        public BsonValue getKey() {
            return key;
        }

        public String getSrcChksum() {
            return srcChksum;
        }

        public String getDestChksum() {
            return destChksum;
        }
    }

    public DiffResult(long matches, long bytesProcessed, Set<MismatchEntry> mismatchedEntries, Set<BsonValue> srcOnlyKeys,
                      Set<BsonValue> destOnlyKeys, Namespace namespace, ChunkDef chunkDef) {
        this.matches = matches;
        this.bytesProcessed = bytesProcessed;
        this.mismatchedEntries = mismatchedEntries;
        this.srcOnlyKeys = srcOnlyKeys;
        this.destOnlyKeys = destOnlyKeys;
        this.namespace = namespace;
        this.chunkDef = chunkDef;
        this.failedKeys = new HashSet<>();
        failedKeys.addAll(this.mismatchedEntries.stream().map(e -> e.key).collect(Collectors.toSet()));
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

    public Set<MismatchEntry> getMismatchedEntries() {
        return mismatchedEntries;
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
