package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

public abstract class AbstractOplogTailWorker implements Runnable {

	protected static final Logger logger = LoggerFactory.getLogger(MultiBufferOplogTailWorker.class);
	protected String shardId;
	protected ShardTimestamp shardTimestamp;
	protected ShardClient sourceShardClient;
	protected ShardClient destShardClient;
	protected MongoSyncOptions options;
	protected OplogTailMonitor oplogTailMonitor;
	protected boolean shutdown;
	protected ApplyOperationsHelper applyOperationsHelper;
	
	protected String currentNs;

	public AbstractOplogTailWorker() {
		super();
	}

	@Override
	public void run() {
		
	    ScheduledExecutorService monitorExecutor = Executors.newScheduledThreadPool(1);
	    monitorExecutor.scheduleAtFixedRate(oplogTailMonitor, 0L, 30L, TimeUnit.SECONDS);
		
		MongoDatabase local = sourceShardClient.getShardMongoClient(shardId).getDatabase("local");
		MongoCollection<BsonDocument> oplog = local.getCollection("oplog.rs", BsonDocument.class);
	
	
		MongoCursor<BsonDocument> cursor = null;
		// TODO - should we ignore no-ops
		// appears that mongomirror does not for keeping the timestamp up to date
		Bson query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n")); 
		//Bson query = gte("ts", shardTimestamp.getTimestamp()); 
		
		long start = System.currentTimeMillis();
		long count;
		try {
			
			logger.debug("{}: starting oplog tail query: {}", shardId, query);
			count = 0;
			cursor = oplog.find(query).sort(eq("$natural", 1))
					.oplogReplay(true)
					.noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext() && !shutdown) {
				BsonDocument doc = cursor.next();
				String op = doc.getString("op").getValue();
				
				if (op.equals("n") || op.equals("c")) {
					continue;
				}
				currentNs = doc.getString("ns").getValue();
				if (currentNs == null || currentNs.equals("") || currentNs.startsWith("config.")) {
					continue;
				}
	
				addToBuffer(doc);
	            count++;
			}
			
	        // flush any remaining from the buffer
	        flushBuffers();
			
			long end = System.currentTimeMillis();
			Double dur = (end - start) / 1000.0;
			logger.debug("{}: oplog tail complete, {} operations applied in {} seconds",  shardId, count, dur);
		
		} catch (Exception e) {
			logger.error("{}: tail error", shardId, e);
		} finally {
	    	try {
	    		logger.debug("{}: finally flush", shardId);
				flushBuffers();
			} catch (MongoException | IOException e) {
				logger.error("final apply error", e);
			}
	
			cursor.close();
		}
		
	}
	
	//protected abstract void flushBuffer(List<BsonDocument> buffer) throws MongoException, IOException;
	
	protected abstract Integer addToBuffer(BsonDocument doc);

	protected abstract void stop();

	protected abstract void flushBuffers() throws MongoException, IOException;

}