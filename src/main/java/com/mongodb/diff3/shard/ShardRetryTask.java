package com.mongodb.diff3.shard;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.DiffConfiguration;
//import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.RetryStatus;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class ShardRetryTask extends ShardDiffTask implements RetryTask {

    public static final ShardRetryTask END_TOKEN = new ShardRetryTask(null, null,
            null, null, null, null, null, null,
            null, null, null);

    private final RetryStatus retryStatus;
    private final Set<BsonValue> failedIds;


    public ShardRetryTask(RetryStatus retryStatus, ShardClient sourceShardClient, ShardClient destShardClient,
                          String srcShardName, String destShardName, Namespace namespace, RawBsonDocument chunk,
                          DiffConfiguration config, Set<BsonValue> failedIds, Queue<RetryTask> retryQueue,
                          DiffSummary summary) {
        super(sourceShardClient, destShardClient, config, chunk, namespace,
                srcShardName, destShardName, retryQueue, summary);
        this.retryStatus = retryStatus;
        this.failedIds = failedIds;
    }

    @Override
    public DiffResult call() throws Exception {
        try {
            logger.trace("[{}] got a retry task", Thread.currentThread().getName());
            start = System.currentTimeMillis();
            DiffResult result = null;
            boolean complete = false;
            if (retryStatus.canRetry()) {
                try {
                    result = computeDiff(failedIds);
                } catch (Exception e) {
                    logger.error("[{}] Fatal error performing diffs, ns: {}",
                            Thread.currentThread().getName(), namespace.getNamespace(), e);
                    throw new RuntimeException(e);
                }

                if (result.getFailedKeys().size() > 0) {
                    RetryStatus newRetryStatus = retryStatus.increment();
                    if (newRetryStatus != null) {
                        RetryTask newRetryTask = createRetryTask(newRetryStatus, result);
                        logger.debug("[{}] retry for ({}) failed ({} ids); submitting attempt {} to retryQueue",
                                Thread.currentThread().getName(), failedIds,
                                result.getFailedKeys().size(), newRetryStatus.getAttempt());

                        summary.updateRetryTask(result);

                        retryQueue.add(newRetryTask);
                    } else {
                        logger.debug("[{}] {} - retry task for ({}) failed; too many retry attempts",
                                Thread.currentThread().getName(), namespace, failedIds);
//                        summary.updateRetryingDone(result);
                        logger.debug("[{}] sending end token for ({})", Thread.currentThread().getName(), unitString());
                        retryQueue.add(endToken());
                        result.setRetryable(false);
                        complete = true;
                    }
                } else {
                    // Retry succeeded
                    logger.info("[{}] {} - retry task for ({}) succeeded on attempt: {}. ids: {}",
                            Thread.currentThread().getName(), namespace, unitString(),
                            retryStatus.getAttempt(), failedIds);
//                    summary.updateRetryingDone(result);
                    logger.debug("[{}] sending end token for ({})", Thread.currentThread().getName(), unitString());
                    retryQueue.add(endToken());
                    complete = true;
                }
            } else {
                // Re-submit
                logger.trace("Requeue {}; not enough time elapse to retry", unitString());
                retryQueue.add(this);
            }

            if (complete) {
                logger.debug("[{}] completed a retry task in {} ms :: {}",
                        Thread.currentThread().getName(), System.currentTimeMillis() - start, result);

                return result;
            }

        } catch (Exception e) {
            logger.error("********* ShardRetryTask call exception", e);
        }

        return null;
    }

    @Override
    protected String unitString() {
        return chunkDef.unitString();
    }

    @Override
    protected ShardRetryTask createRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new ShardRetryTask(retryStatus, sourceShardClient, destShardClient, srcShardName, destShardName,
                namespace, chunk, config, new HashSet<>(failedIds), retryQueue, summary);
    }

    @Override
    protected RetryTask endToken() {
        return END_TOKEN;
    }

    @Override
    protected MongoClient getLoadClient(Target target) {
        ShardClient shardClient;
        String shardName;
        switch (target) {
            case SOURCE:
                shardClient = sourceShardClient;
                shardName = srcShardName;
                break;
            case DEST:
                shardClient = destShardClient;
                shardName = destShardName;
                break;
            default:
                throw new RuntimeException("Unexpected target type: " + target.getName());
        }
        return shardClient.getShardMongoClient(shardName);
    }

    @Override
    public RetryStatus getRetryStatus() {
        return retryStatus;
    }
}
