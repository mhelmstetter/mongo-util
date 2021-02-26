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

	
	//protected abstract void flushBuffer(List<BsonDocument> buffer) throws MongoException, IOException;
	
	protected abstract void addToBuffer(BsonDocument doc) throws InterruptedException;

	protected abstract void stop();


}