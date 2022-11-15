package com.mongodb.diff3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

public class PartitionRetryTask implements Callable<PartitionDiffResult> {
    public static final PartitionRetryTask END_TOKEN =
            new PartitionRetryTask(null, null, null, null, null);
    private final RetryStatus retryStatus;
    private final PartitionDiffTask originalTask;
    private final PartitionDiffResult originalResult;
    private final Set<String> failedIds;
    private final Queue<PartitionRetryTask> retryQueue;
    private long start;
    private static final Logger logger = LoggerFactory.getLogger(PartitionRetryTask.class);

    public PartitionRetryTask(RetryStatus retryStatus, PartitionDiffTask originalTask,
                              PartitionDiffResult originalResult, Set<String> failedIds,
                              Queue<PartitionRetryTask> retryQueue) {
        this.retryStatus = retryStatus;
        this.originalTask = originalTask;
        this.originalResult = originalResult;
        this.failedIds = failedIds;
        this.retryQueue = retryQueue;
    }

    @Override
    public PartitionDiffResult call() throws Exception {
        logger.debug("[{}] got a retry task", Thread.currentThread().getName());
        start = System.currentTimeMillis();
        PartitionDiffResult result = null;
        while (true) {
            if (retryStatus.canRetry()) {
                PartitionDiffResult copyOfOriginalResult = originalResult.copy();
                try {
                    result = originalTask.computeDiff(failedIds);
                } catch (Exception e) {
                    logger.error("Fatal error performing diffs, partition: {}",
                            originalTask.getPartition());
                    throw new RuntimeException(e);
                } finally {
                    originalTask.closeCursor(originalTask.sourceCursor);
                    originalTask.closeCursor(originalTask.destCursor);
                }
                result = result.mergeRetryResult(copyOfOriginalResult);

                if (result.getFailureCount() > 0) {
                    RetryStatus newRetryStatus = retryStatus.increment();
                    if (newRetryStatus != null) {
                        PartitionRetryTask newRetryTask = new PartitionRetryTask(
                                newRetryStatus, originalTask, result, result.getFailedIds(), retryQueue);
                        logger.debug("[{}] retry for ({}) failed ({} ids); submitting attempt {} to retryQueue",
                                Thread.currentThread().getName(), originalTask.getPartition().toString(),
                                result.getFailureCount(), newRetryStatus.getAttempt());
                        retryQueue.add(newRetryTask);
                    } else {
                        logger.info("[{}] retry task for ({}) failed; too many retry attempts",
                                Thread.currentThread().getName(), originalTask.getPartition().toString());
                        retryQueue.add(END_TOKEN);
                        result.setRetryable(false);
                    }
                } else {
                    // Retry succeeded
                    logger.info("[{}] retry task for ({}) succeeded on attempt: {}",
                            Thread.currentThread().getName(), originalTask.getPartition().toString(),
                            retryStatus.getAttempt());
                    retryQueue.add(END_TOKEN);
                }
                break;
            } else {
                Thread.sleep(1000);
            }
        }


        logger.debug("[{}] completed a retry task in {} ms :: {}",
                Thread.currentThread().getName(), System.currentTimeMillis() - start, result);

        return result.copy();
    }

    public RetryStatus getRetryStatus() {
        return retryStatus;
    }

    public PartitionDiffTask getOriginalTask() {
        return originalTask;
    }
}
