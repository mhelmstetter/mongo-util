package com.mongodb.diff3;

import java.util.concurrent.Callable;

import org.bson.BsonDocument;

import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class UnshardedDiffTask extends AbstractDiffTask implements Callable<DiffResult> {
    

    public UnshardedDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, String nsStr) {
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.namespace = new Namespace(nsStr);
    }

    @Override
    public DiffResult call() throws Exception {
        logger.debug("Thread [{}] got an unsharded task", Thread.currentThread().getName());
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

        logger.debug("Thread [{}] completed an unsharded task in {} ms", Thread.currentThread().getName(),
                timeSpent(System.currentTimeMillis()));
        return result;
    }

}
