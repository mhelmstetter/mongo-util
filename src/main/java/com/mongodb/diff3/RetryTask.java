package com.mongodb.diff3;

import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

public abstract class RetryTask implements Callable<DiffResult> {

    protected RetryStatus retryStatus;
    protected DiffTask originalTask;
    protected DiffResult originalResult;
    protected Set<BsonValue> failedIds;
    protected Queue<RetryTask> retryQueue;
    protected DiffSummary summary;
    protected static final Logger logger = LoggerFactory.getLogger(RetryTask.class);
    protected long start;


    public RetryTask(RetryStatus retryStatus, DiffTask originalTask, DiffResult originalResult,
                     Set<BsonValue> failedIds, Queue<RetryTask> retryQueue, DiffSummary summary) {
        this.retryStatus = retryStatus;
        this.originalTask = originalTask;
        this.originalResult = originalResult;
        this.failedIds = failedIds;
        this.retryQueue = retryQueue;
        this.summary = summary;
    }

    @Override
    public DiffResult call() throws Exception {
    	try {
    		logger.trace("[{}] got a retry task", Thread.currentThread().getName());
            start = System.currentTimeMillis();
            DiffResult result = null;
            boolean complete = false;
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
                        RetryTask newRetryTask = initRetryTask(newRetryStatus, result);
                        logger.debug("[{}] retry for ({}) failed ({} ids); submitting attempt {} to retryQueue",
                                Thread.currentThread().getName(), originalTask.unitLogString(),
                                result.getFailureCount(), newRetryStatus.getAttempt());
                        retryQueue.add(newRetryTask);
                    } else {
                        logger.info("[{}] retry task for ({}) failed; too many retry attempts",
                                Thread.currentThread().getName(), originalTask.unitLogString());
                        summary.updateRetryingDone(result);
                        logger.debug("[{}] sending end token for ({})", Thread.currentThread().getName(),
                                originalTask.unitLogString());
                        retryQueue.add(endToken());
                        result.setRetryable(false);
                        complete = true;
                    }
                } else {
                    // Retry succeeded
                    logger.info("[{}] retry task for ({}) succeeded on attempt: {}",
                            Thread.currentThread().getName(), originalTask.unitLogString(),
                            retryStatus.getAttempt());
                    summary.updateRetryingDone(result);
                    logger.debug("[{}] sending end token for ({})", Thread.currentThread().getName(),
                            originalTask.unitLogString());
                    retryQueue.add(endToken());
                    complete = true;
                }
            } else {
                // Re-submit
                logger.trace("Requeue {}; not enough time elapse to retry", originalTask.unitLogString());
                retryQueue.add(this);
            }

            if (complete) {
                logger.debug("[{}] completed a retry task in {} ms :: {}",
                        Thread.currentThread().getName(), System.currentTimeMillis() - start, result);

                return result.copy();
            }
    		
    	} catch (Exception e) {
    		logger.error("********* call exception", e);
    	}
        
        return null;
    }

    protected abstract RetryTask endToken();

    protected abstract RetryTask initRetryTask(RetryStatus retryStatus, DiffResult result);

    public DiffTask getOriginalTask() {
        return originalTask;
    }

    public RetryStatus getRetryStatus() {
        return retryStatus;
    }
}
