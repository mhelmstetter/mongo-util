package com.mongodb.diff3.shard;

import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary2;
import com.mongodb.diff3.RetryTask;
import com.mongodb.diff3.RetryStatus;
import org.bson.BsonValue;

import java.util.Queue;
import java.util.Set;

public class ShardRetryTask extends RetryTask {

    public static final ShardRetryTask END_TOKEN = new ShardRetryTask(null, null
            , null, null, null, null);


    public ShardRetryTask(RetryStatus retryStatus, ShardDiffTask originalTask, ShardDiffResult originalResult,
                          Set<BsonValue> failedIds, Queue<RetryTask> retryQueue, DiffSummary2 summary) {
        super(retryStatus, originalTask, originalResult, failedIds, retryQueue, summary);
    }

    @Override
    protected ShardRetryTask endToken() {
        return END_TOKEN;
    }

    @Override
    protected ShardRetryTask initRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new ShardRetryTask(retryStatus, (ShardDiffTask) originalTask, (ShardDiffResult) result,
                result.getFailedKeys(), retryQueue, summary);
    }
}
