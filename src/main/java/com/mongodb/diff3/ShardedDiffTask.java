package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

import static com.mongodb.client.model.Filters.*;

public class ShardedDiffTask extends AbstractDiffTask implements Callable<DiffResult> {


    private RawBsonDocument chunk;
    private Queue<RetryTask> retryQueue;


    public ShardedDiffTask(ShardClient sourceShardClient, ShardClient destShardClient,
                           DiffConfiguration config, RawBsonDocument chunk, String srcShardName,
                           String destShardName, Queue<RetryTask> retryQueue) {
        super();
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.config = config;
        this.chunk = chunk;
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
        this.retryQueue = retryQueue;
    }

    @Override
    public DiffResult call() throws Exception {
        DiffResult output;
        this.start = System.currentTimeMillis();

        BsonDocument min = chunk.getDocument("min");
        BsonDocument max = chunk.getDocument("max");
        String nsStr = chunk.getString("ns").getValue();
        this.namespace = new Namespace(nsStr);
        chunkString = "[" + min.toString() + " : " + max.toString() + "]";

        ShardedDiffResult result = new ShardedDiffResult();
        result.setChunk(chunk);
        result.chunkString = chunkString;

        logger.debug("[{}] got a sharded task ({}-{})", Thread.currentThread().getName(),
                namespace.getNamespace(), chunkString);
        Document shardCollection = sourceShardClient.getCollectionsMap().get(nsStr);
        Document shardKeysDoc = (Document) shardCollection.get("key");
        Set<String> shardKeys = shardKeysDoc.keySet();

        if (shardKeys.size() > 1) {
            List<Bson> filters = new ArrayList<Bson>(shardKeys.size());
            for (String key : shardKeys) {
                BsonValue minkey = min.get(key);
                BsonValue maxkey = max.get(key);
                if (minkey.equals(maxkey)) {
                    filters.add(eq(key, minkey));
                } else {
                    filters.add(and(gte(key, minkey), lt(key, maxkey)));
                }
            }
            query = and(filters);
        } else {
            String key = shardKeys.iterator().next();
            query = and(gte(key, min.get(key)), lt(key, max.get(key)));
        }
        result.ns = nsStr;
        result.setChunkQuery(query);
        this.result = result;

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
            RetryTask retryTask = new RetryTask(retryStatus, this, result, result.failedIds, retryQueue);
            retryQueue.add(retryTask);
            logger.debug("[{}] detected {} failures and added a retry task ({}-{})",
                    Thread.currentThread().getName(), result.getFailureCount(),
                    namespace.getNamespace(), chunkString);
//            output = null;
            output = result;
        } else {
            output = result;
            retryQueue.add(RetryTask.END_TOKEN);
        }

        if (output != null) {
            long timeSpent = timeSpent(System.currentTimeMillis());
            logger.debug("[{}] completed a sharded task in {} ms ({}-{}) :: {}",
                    Thread.currentThread().getName(), timeSpent,
                    namespace.getNamespace(), chunkString, result.shortString());
        }
        return output;
    }

}
