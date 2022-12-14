package com.mongodb.diff3.shard;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.ChunkDef;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.DiffTask;
import com.mongodb.diff3.RetryStatus;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;

import java.util.Queue;

public class ShardDiffTask extends DiffTask {

    protected final ShardClient sourceShardClient;
    protected final ShardClient destShardClient;

    protected final String srcShardName;
    protected final String destShardName;

    protected final RawBsonDocument chunk;

    public ShardDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, DiffConfiguration config,
                         RawBsonDocument chunk, Namespace namespace, String srcShardName,
                         String destShardName, Queue<RetryTask> retryQueue, DiffSummary summary) {
        super(config, namespace, retryQueue, summary);
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.chunk = chunk;
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
        this.chunkDef = findChunkBounds();
    }

    private ChunkDef findChunkBounds() {
        BsonDocument min = chunk != null ? chunk.getDocument("min") : null;
        BsonDocument max = chunk != null ? chunk.getDocument("max") : null;
//        chunkString = "[" + min.toString() + " : " + max.toString() + "]";
        return new ChunkDef(namespace, min, max);
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

//    protected Pair<Bson, Bson> getChunkBounds() {
//        return (chunk != null) ? findChunkBounds() : null;
//    }

    @Override
    protected String unitString() {
        return chunkDef.unitString();
    }

    @Override
    protected RetryTask endToken() {
        return ShardRetryTask.END_TOKEN;
    }

    @Override
    protected ShardRetryTask createRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new ShardRetryTask(retryStatus, sourceShardClient, destShardClient, srcShardName, destShardName,
                namespace, chunk, config, result.getFailedKeys(), retryQueue, summary);
    }
}