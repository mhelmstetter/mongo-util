package com.mongodb.diff3;


import com.mongodb.model.Namespace;

import java.util.HashSet;

public class PartitionDiffResult extends DiffResult {
    Partition partition;
    Namespace namespace;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("DiffResult [ns=");
        builder.append(namespace);
        builder.append(", matches=");
        builder.append(matches);
        builder.append(", failedIds=");
        builder.append(failedIds);
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

    public void add(PartitionDiffResult subResult) {
        matches += subResult.matches;
        onlyOnSource += subResult.onlyOnSource;
        onlyOnDest += subResult.onlyOnDest;
        bytesProcessed += subResult.bytesProcessed;
        if (failedIds == null) {
            failedIds = new HashSet<>();
        }
        if (subResult.failedIds != null) {
            failedIds.addAll(subResult.failedIds);
        }
    }
}
