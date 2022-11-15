package com.mongodb.diff3;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

public class RetryTask implements Callable<DiffResult> {

    public static final RetryTask END_TOKEN = new RetryTask(null, null
            , null, null, null);
    private final RetryStatus retryStatus;
    private final DiffTask originalTask;
    private final DiffResult originalResult;
    private final Set<String> failedIds;
    private final Queue<RetryTask> retryQueue;
    private static final Logger logger = LoggerFactory.getLogger(RetryTask.class);
    private long start;


    public RetryTask(RetryStatus retryStatus, DiffTask originalTask,
                     DiffResult originalResult, Set<String> failedIds, Queue<RetryTask> retryQueue) {
        this.retryStatus = retryStatus;
        this.originalTask = originalTask;
        this.originalResult = originalResult;
        this.failedIds = failedIds;
        this.retryQueue = retryQueue;
    }

    @Override
    public DiffResult call() throws Exception {
        logger.debug("[{}] got a retry task", Thread.currentThread().getName());
        start = System.currentTimeMillis();
        DiffResult result = null;
        while (true) {
            if (retryStatus.canRetry()) {
                DiffResult copyOfOriginalResult = originalResult.copy();
                try {
                    result = originalTask.computeDiff(failedIds);
                } catch (Exception e) {
                    logger.error("[{}] Fatal error performing diffs, ns: {}",
                            Thread.currentThread().getName(), originalTask.getNamespace(), e);
                    throw new RuntimeException(e);
                } finally {
                    originalTask.closeCursor(originalTask.sourceCursor);
                    originalTask.closeCursor(originalTask.destCursor);
                }
                result = result.mergeRetryResult(copyOfOriginalResult);

                if (result.getFailureCount() > 0) {
                    RetryStatus newRetryStatus = retryStatus.increment();
                    if (newRetryStatus != null) {
                        RetryTask newRetryTask = new RetryTask(
                                newRetryStatus, originalTask, result, result.getFailedIds(), retryQueue);
                        logger.debug("[{}] retry for ({}-{}) failed ({} ids); submitting attempt {} to retryQueue",
                                Thread.currentThread().getName(), originalTask.getNamespace(),
                                originalTask.chunkString, result.getFailureCount(), newRetryStatus.getAttempt());
                        retryQueue.add(newRetryTask);
                    } else {
                        logger.info("[{}] retry task for ({}-{}) failed; too many retry attempts",
                                Thread.currentThread().getName(), originalTask.getNamespace(),
                                originalTask.chunkString);
                        retryQueue.add(END_TOKEN);
                        result.setRetryable(false);
                    }
                } else {
                    // Retry succeeded
                    logger.info("[{}] retry task for ({}-{}) succeeded on attempt: {}",
                            Thread.currentThread().getName(), originalTask.getNamespace(), originalTask.chunkString,
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

    public DiffTask getOriginalTask() {
        return originalTask;
    }

    public RetryStatus getRetryStatus() {
        return retryStatus;
    }
}
