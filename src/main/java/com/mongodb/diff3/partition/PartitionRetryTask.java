package com.mongodb.diff3.partition;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.RetryStatus;
import com.mongodb.diff3.RetryTask;
import org.bson.BsonValue;

import java.util.Queue;
import java.util.Set;

public class PartitionRetryTask extends PartitionDiffTask implements RetryTask {
    public static final PartitionRetryTask END_TOKEN =
            new PartitionRetryTask(null, null, null, null, null,
                    null, null, null);

    private final RetryStatus retryStatus;
    private final Set<BsonValue> failedIds;

    public PartitionRetryTask(Partition partition, MongoClient sourceClient, MongoClient destClient,
                              Queue<RetryTask> retryQueue, DiffSummary summary, DiffConfiguration config,
                              RetryStatus retryStatus, Set<BsonValue> failedIds) {
        super(partition, sourceClient, destClient, retryQueue, summary, config);
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
                } finally {
                    closeCursor(sourceCursor);
                    closeCursor(destCursor);
                }

                if (result.getFailedKeys().size() > 0) {
                    RetryStatus newRetryStatus = retryStatus.increment();
                    if (newRetryStatus != null) {
                        RetryTask newRetryTask = createRetryTask(newRetryStatus, result);
                        logger.debug("[{}] retry for ({}) failed ({} ids); submitting attempt {} to retryQueue",
                                Thread.currentThread().getName(), failedIds,
                                result.getFailedKeys().size(), newRetryStatus.getAttempt());
                        retryQueue.add(newRetryTask);
                    } else {
                        logger.debug("[{}] {} - retry task for ({}) failed; too many retry attempts",
                                Thread.currentThread().getName(), namespace, failedIds);
                        summary.updateRetryingDone(result);
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
                    summary.updateRetryingDone(result);
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
            logger.error("********* call exception", e);
        }

        return null;
    }

    @Override
    protected PartitionRetryTask endToken() {
        return END_TOKEN;
    }

    @Override
    protected PartitionRetryTask createRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new PartitionRetryTask(partition, sourceClient, destClient, retryQueue,
                summary, config, retryStatus, failedIds);
    }

    @Override
    public RetryStatus getRetryStatus() {
        return retryStatus;
    }
}
