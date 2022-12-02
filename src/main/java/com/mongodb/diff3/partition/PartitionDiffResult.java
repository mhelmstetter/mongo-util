package com.mongodb.diff3.partition;


import com.mongodb.diff3.DiffResult;

import java.util.HashSet;

public class PartitionDiffResult extends DiffResult {
    private Partition partition;

    public PartitionDiffResult() {
        super();
    }

    public PartitionDiffResult(DiffResult rr) {
        assert rr instanceof PartitionDiffResult;
        PartitionDiffResult prr = (PartitionDiffResult) rr;
        this.matches = prr.matches;
        this.keysOnlyOnSource = prr.keysOnlyOnSource;
        this.keysOnlyOnDest = prr.keysOnlyOnDest;
        this.failedKeys = prr.failedKeys;
        this.bytesProcessed = prr.bytesProcessed;
        this.namespace = prr.namespace;
        this.retryable = prr.retryable;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiffResult [ns=");
        builder.append(namespace.getNamespace());
        builder.append(", matches=");
        builder.append(getMatches());
        builder.append(", failedIds=");
        builder.append(failedKeys == null ? 0 : failedKeys.size());
        if (partition != null) {
            builder.append(", partitionBounds=[");
            builder.append(partition.getLowerBound()).append(", ").append(partition.getUpperBound());
            builder.append("]");
        }
        return builder.toString();
    }

    @Override
    public String shortString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ns=");
        sb.append(namespace);
        if (partition != null) {
            sb.append(", partitionBounds=[");
            sb.append(partition.getLowerBound()).append(", ").append(partition.getUpperBound());
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public PartitionDiffResult mergeRetryResult(DiffResult rr) {
        assert rr instanceof PartitionDiffResult;
        PartitionDiffResult prr = (PartitionDiffResult) rr;
        PartitionDiffResult merged = new PartitionDiffResult();
        merged.matches = prr.matches + this.matches;
        merged.keysOnlyOnSource = prr.keysOnlyOnSource;
        merged.keysOnlyOnDest = prr.keysOnlyOnDest;
        merged.failedKeys = prr.failedKeys;
        merged.bytesProcessed = prr.bytesProcessed;
        merged.namespace = prr.namespace;
        merged.retryable = prr.retryable;
        merged.partition = prr.partition;
        return merged;
    }

    @Override
    public PartitionDiffResult copy() {
        PartitionDiffResult copy = new PartitionDiffResult();
        copy.matches = this.matches;
        copy.keysOnlyOnSource = this.keysOnlyOnSource;
        copy.keysOnlyOnDest = this.keysOnlyOnDest;
        copy.bytesProcessed = this.bytesProcessed;
        copy.namespace = this.namespace;
        copy.retryable = this.retryable;
        if (failedKeys == null) {
            copy.failedKeys = new HashSet<>();
        } else {
            copy.failedKeys = new HashSet<>(failedKeys);
        }
        copy.partition = this.partition;
        return copy;
    }

    @Override
    public String unitLogString() {
        return partition.toString();
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }
}
