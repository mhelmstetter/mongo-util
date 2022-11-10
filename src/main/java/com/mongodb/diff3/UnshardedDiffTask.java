package com.mongodb.diff3;

import java.util.Queue;
import java.util.concurrent.Callable;

import org.bson.BsonDocument;

import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class UnshardedDiffTask extends AbstractDiffTask implements Callable<DiffResult> {

    private Queue<RetryTask> retryQueue;

    public UnshardedDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, String nsStr,
                             String srcShardName, String destShardName, Queue<RetryTask> retryQueue) {
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.namespace = new Namespace(nsStr);
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
        this.retryQueue = retryQueue;
    }

    @Override
    public DiffResult call() throws Exception {
        DiffResult output;
        logger.debug("Thread [{}--{}] got an unsharded task", Thread.currentThread().getName(), srcShardName);
        this.start = System.currentTimeMillis();
        result = new UnshardedDiffResult(namespace.getNamespace());
        query = new BsonDocument();

        try {
            computeDiff();
        } catch (Exception me) {
            logger.error("fatal error diffing chunk, ns: {}", namespace, me);
            result = null;
        } finally {
            closeCursor(sourceCursor);
            closeCursor(destCursor);
        }

        if (result.getFailureCount() > 0) {
            RetryStatus retryStatus = new RetryStatus(0, System.currentTimeMillis());
            RetryTask retryTask = new RetryTask(retryStatus, this, result, retryQueue);
            retryQueue.add(retryTask);
            output = null;
        } else {
            output = result;
        }

        if (output != null) {
            logger.debug("Thread [{}--{}] completed an unsharded task in {} ms", Thread.currentThread().getName(),
                    srcShardName, timeSpent(System.currentTimeMillis()));
        }
        return output;
    }

}
