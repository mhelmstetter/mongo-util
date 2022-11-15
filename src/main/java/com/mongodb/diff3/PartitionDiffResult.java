package com.mongodb.diff3;


import com.mongodb.model.Namespace;

import java.util.HashSet;

public class PartitionDiffResult extends DiffResult {
    private Partition partition;
    private Namespace namespace;

    public PartitionDiffResult() {
        super();
    }

    public PartitionDiffResult(DiffResult base) {
        this.matches = base.matches;
        this.onlyOnSource = base.onlyOnSource;
        this.onlyOnDest = base.onlyOnDest;
        this.failedIds = base.failedIds;
        this.bytesProcessed = base.bytesProcessed;
        this.retryable = base.retryable;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiffResult [ns=");
        builder.append(namespace);
        builder.append(", matches=");
        builder.append(getMatches());
        builder.append(", failedIds=");
        builder.append(failedIds == null ? 0 : failedIds.size());
        if (partition != null) {
            builder.append(", partitionBounds=[");
            builder.append(partition.getLowerBound() + ", " + partition.getUpperBound());
            builder.append("]");
        }
        return builder.toString();
    }

    public String shortString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ns=");
        sb.append(namespace);
        if (partition != null) {
            sb.append(", partitionBounds=[");
            sb.append(partition.getLowerBound() + ", " + partition.getUpperBound());
            sb.append("]");
        }
        return sb.toString();
    }

    @Override
    public PartitionDiffResult mergeRetryResult(DiffResult rr) {
        PartitionDiffResult merged = new PartitionDiffResult(super.mergeRetryResult(rr));
        merged.partition = ((PartitionDiffResult) rr).partition;
        merged.namespace = ((PartitionDiffResult) rr).namespace;
        return merged;
    }

    @Override
    public PartitionDiffResult copy() {
        PartitionDiffResult copy = new PartitionDiffResult(super.copy());
        copy.namespace = this.namespace;
        copy.partition = this.partition;
        return copy;
    }

    public Partition getPartition() {
        return partition;
    }

    public void setPartition(Partition partition) {
        this.partition = partition;
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public void setNamespace(Namespace namespace) {
        this.namespace = namespace;
    }
}
