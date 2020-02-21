package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.List;

import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

public class OplogTailWorker implements Runnable {
    
    protected static final Logger logger = LoggerFactory.getLogger(OplogTailWorker.class);
    
    private String shardId;
    private ShardTimestamp shardTimestamp;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    private DiffOptions options;
    
    public OplogTailWorker(ShardTimestamp shardTimestamp, ShardClient sourceShardClient, ShardClient destShardClient, DiffOptions options) {
        this.shardId = shardTimestamp.getShardName();
        this.shardTimestamp = shardTimestamp;
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.options = options;
    }

    @Override
    public void run() {
        MongoDatabase local = sourceShardClient.getShardMongoClient(shardId).getDatabase("local");
        MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);
        
        //List<RawBsonDocument> buffer = new ArrayList<RawBsonDocument>(options.getBatchSize());
        
        MongoCursor<RawBsonDocument> cursor = null;
        Bson query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"));
        long start = System.currentTimeMillis();
        long count = 0;
        try {
            cursor = oplog.find(query).noCursorTimeout(true).cursorType(CursorType.TailableAwait).iterator();
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                String ns = doc.getString("ns").getValue();
                
                
                
            }

            
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        //logger.debug(String.format("\nDone cloning %s, %s documents in %f seconds", ns, count, dur));
    }

}
