package com.mongodb.mongosync;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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