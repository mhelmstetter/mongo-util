package com.mongodb.mongosync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

public class OplogTailWorker extends AbstractOplogTailWorker implements Runnable {

	protected static final Logger logger = LoggerFactory.getLogger(OplogTailWorker.class);

	List<BsonDocument> buffer;

	public OplogTailWorker(ShardTimestamp shardTimestamp, TimestampFile timestampFile, ShardClient sourceShardClient, ShardClient destShardClient,
			MongoSyncOptions options) throws IOException {
		this.shardId = shardTimestamp.getShardName();
		this.shardTimestamp = shardTimestamp;
		this.sourceShardClient = sourceShardClient;
		this.options = options;
		oplogTailMonitor = new OplogTailMonitor(timestampFile, sourceShardClient, null);
		this.applyOperationsHelper = new ApplyOperationsHelper(shardId, oplogTailMonitor, destShardClient);
		buffer = new ArrayList<BsonDocument>(options.getBatchSize());
	}

	
	
	public synchronized void stop() {
		logger.debug("{}: OplogTailWorker got stop signal", shardId);
		this.shutdown = true;
	}

	private synchronized void flushBuffer(List<BsonDocument> buffer) throws MongoException {
		if (buffer.size() > 0) {
			try {
				applyOperationsHelper.applyOperations(buffer);
			} catch (Exception e) {
				logger.error("flushBuffer error", e);
			}
		}
	}

	@Override
	protected Integer addToBuffer(BsonDocument doc) {
		buffer.add(doc);
		
		if (buffer.size() >= options.getBatchSize()) {
        	flushBuffer(buffer);
        }
		return null;
	}

	@Override
	protected void flushBuffers() throws MongoException {
		flushBuffer(buffer);
	}

}
