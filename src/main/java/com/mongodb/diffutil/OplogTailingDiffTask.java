package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Namespace;
import com.mongodb.model.OplogSummary;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.oplog.OplogUtil;
import com.mongodb.shardsync.ShardClient;

public class OplogTailingDiffTask implements Callable<ShardTimestamp> {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogTailingDiffUtil.class);
	
	private final static Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
	
	private final static int batchSize = 100;
    
    private String shardName;
    private ShardClient client;
    
    private ExecutorService executor;
    
    public OplogTailingDiffTask(String shardName, ShardClient client) {
        this.shardName = shardName;
        this.client = client;
    }

    
    @Override
    public ShardTimestamp call() throws Exception {
        
    	ShardTimestamp shardTimestamp = client.populateLatestOplogTimestamp(shardName);
    	
    	MongoDatabase local = client.getShardMongoClient(shardName).getDatabase("local");
        MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);
        
        //List<RawBsonDocument> buffer = new ArrayList<RawBsonDocument>();
        
        MongoCursor<RawBsonDocument> cursor = null;
        Set<Object> buffer = new HashSet<>();
        Bson query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"));
        long start = System.currentTimeMillis();
        long count = 0;
        try {
            cursor = oplog.find(query).noCursorTimeout(true).cursorType(CursorType.TailableAwait).iterator();
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                
                OplogSummary oplogSummary = OplogUtil.getOplogSummaryFromOplogEntry(doc);
                if (oplogSummary.getId() != null) {
                	logger.debug(String.format("%s - oplog entry: %s", shardName, oplogSummary));
                	
                	buffer.add(oplogSummary.getId());
                	if (buffer.size() > batchSize) {
                		
                	}
                }
            }
            
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        //logger.debug(String.format("\nDone cloning %s, %s documents in %f seconds", ns, count, dur));
        return shardTimestamp;
    }

}
