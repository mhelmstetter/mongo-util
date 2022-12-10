package com.mongodb.diff3.partition;

import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary2;
import com.mongodb.diff3.RetryTask;
import com.mongodb.diff3.RetryStatus;
import org.bson.BsonValue;

import java.util.Queue;
import java.util.Set;

public class PartitionRetryTask extends RetryTask {
    public static final PartitionRetryTask END_TOKEN =
            new PartitionRetryTask(null, null, null, null, null, null);

    public PartitionRetryTask(RetryStatus retryStatus, PartitionDiffTask originalTask,
                              PartitionDiffResult originalResult, Set<BsonValue> failedIds,
                              Queue<RetryTask> retryQueue, DiffSummary2 summary) {
        super(retryStatus, originalTask, originalResult, failedIds, retryQueue, summary);
    }

    @Override
    protected PartitionRetryTask endToken() {
        return END_TOKEN;
    }

    @Override
    protected PartitionRetryTask initRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new PartitionRetryTask(retryStatus, (PartitionDiffTask) originalTask,
                (PartitionDiffResult) originalResult, failedIds, retryQueue, summary);
    }
}
