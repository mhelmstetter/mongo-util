package com.mongodb.diff3.partition;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.DiffTask;
import com.mongodb.diff3.RetryStatus;
import com.mongodb.diff3.RetryTask;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;

import java.util.Queue;


public class PartitionDiffTask extends DiffTask {


    protected final Partition partition;
    protected final MongoClient sourceClient;
    protected final MongoClient destClient;
    public static final PartitionDiffTask END_TOKEN = new PartitionDiffTask(
            null, null, null, null, null, null);


    public PartitionDiffTask(Partition partition, MongoClient sourceClient, MongoClient destClient,
                             Queue<RetryTask> retryQueue, DiffSummary summary, DiffConfiguration config) {
        super(config, (partition == null) ? null : partition.getNamespace(), retryQueue, summary);
        this.partition = partition;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        if (this.partition != null) {
            this.chunkDef = this.partition.toChunkDef();
        }
    }

    //    @Override
    public Bson getPartitionDiffQuery() {
        return (partition != null) ? partition.query() : new BsonDocument();
    }

//    @Override
//    protected Pair<Bson, Bson> getChunkBounds() {
//        return null;
//    }

    @Override
    protected String unitString() {
        return partition.toString();
    }

    @Override
    protected PartitionRetryTask endToken() {
        return PartitionRetryTask.END_TOKEN;
    }

    @Override
    protected MongoClient getLoadClient(Target target) {
        switch (target) {
            case SOURCE:
                return sourceClient;
            case DEST:
                return destClient;
            default:
                throw new RuntimeException("Unknown target: " + target);
        }
    }

    @Override
    protected PartitionRetryTask createRetryTask(RetryStatus retryStatus, DiffResult result) {
        return new PartitionRetryTask(partition, sourceClient, destClient, retryQueue,
                summary, config, retryStatus, result.getFailedKeys());
    }

    public Partition getPartition() {
        return partition;
    }
}
