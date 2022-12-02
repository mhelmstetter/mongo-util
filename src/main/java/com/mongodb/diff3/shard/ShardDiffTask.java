package com.mongodb.diff3.shard;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.*;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import static com.mongodb.client.model.Filters.*;

public class ShardDiffTask extends DiffTask {

    private final ShardClient sourceShardClient;
    private final ShardClient destShardClient;

    private final String srcShardName;
    private final String destShardName;

    private final RawBsonDocument chunk;
    private String chunkString = "[:]";

    public ShardDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, DiffConfiguration config,
                         RawBsonDocument chunk, Namespace namespace, String srcShardName,
                         String destShardName, Queue<RetryTask> retryQueue, DiffSummary summary) {
        super(config, namespace, retryQueue, summary);
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.chunk = chunk;
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
    }

    private Pair<Bson, Bson> findChunkBounds() {
        Bson query;
        BsonDocument min = chunk.getDocument("min");
        BsonDocument max = chunk.getDocument("max");
        chunkString = "[" + min.toString() + " : " + max.toString() + "]";

        return Pair.of(min, max);
        /*Document shardCollection = sourceShardClient.getCollectionsMap().get(namespace.getNamespace());
        Document shardKeysDoc = (Document) shardCollection.get("key");
        Set<String> shardKeys = shardKeysDoc.keySet();

        if (shardKeys.size() > 1) {
            List<Bson> filters = new ArrayList<>(shardKeys.size());
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
        return query;*/
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

//    @Override
//    protected Bson getDiffQuery() {
//        return (chunk != null) ? findChunkBounds() : new BsonDocument();
//    }

    protected Pair<Bson, Bson> getChunkBounds() {
        return (chunk != null) ? findChunkBounds() : null;
    }

    @Override
    protected String unitLogString() {
        return namespace + "-" + chunkString;
    }

    @Override
    protected DiffResult initDiffResult() {
        ShardDiffResult result = new ShardDiffResult();
        result.setNamespace(namespace);
        result.setChunkString(chunkString);
        return result;
    }

    @Override
    protected ShardRetryTask endToken() {
        return ShardRetryTask.END_TOKEN;
    }

    @Override
    protected ShardRetryTask createRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new ShardRetryTask(retryStatus, this, (ShardDiffResult) result,
                result.getFailedKeys(), retryQueue, summary);
    }

    public String getChunkString() {
        return chunkString;
    }
}