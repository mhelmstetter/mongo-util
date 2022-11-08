package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
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


    public ShardedDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, DiffConfiguration config,
                           RawBsonDocument chunk, String srcShardName, String destShardName) {
        super();
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.config = config;
        this.chunk = chunk;
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
    }

    @Override
    public DiffResult call() throws Exception {
        logger.debug("Thread [{}-{}] got a sharded task", Thread.currentThread().getName(), srcShardName);
        this.start = System.currentTimeMillis();
        ShardedDiffResult result = new ShardedDiffResult();
        result.setChunk(chunk);

        BsonDocument min = chunk.getDocument("min");
        BsonDocument max = chunk.getDocument("max");
        String nsStr = chunk.getString("ns").getValue();
        this.namespace = new Namespace(nsStr);

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
        result.setNS(nsStr);
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
        long timeSpent = timeSpent(System.currentTimeMillis());
        logger.debug("Thread [{}--{}] completed a sharded task in {} ms", Thread.currentThread().getName(),
                srcShardName, timeSpent);
        return result;
    }

}
