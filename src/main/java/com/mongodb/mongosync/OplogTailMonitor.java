package com.mongodb.mongosync;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardClient;

public class OplogTailMonitor implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogTailMonitor.class);
	
	private long duplicateKeyExceptionCount;
    private long deletedCount;
    private long modifiedCount;
    private long insertedCount;
    private long upsertedCount;
    //private long failedOpsCount;
	
	BsonTimestamp latestTimestamp;
	
	private TimestampFile timestampFile;
	
	private ShardClient sourceShardClient;
	//private ClientSession sourceSession;
	private String shardId;
	
	private Map<Integer, BlockingQueue<OplogQueueEntry>> childQueues;
	
	public OplogTailMonitor(TimestampFile timestampFile, ShardClient sourceShardClient, Map<Integer, BlockingQueue<OplogQueueEntry>> childQueues) {
		this.timestampFile = timestampFile;
		this.sourceShardClient = sourceShardClient;
		this.shardId = timestampFile.getShardId();
		this.childQueues = childQueues;
		//this.worker = worker;
		//this.sourceSession = sourceShardClient.getShardMongoClient(shardId).startSession();
	}
	
	protected synchronized void setLatestTimestamp(BsonTimestamp ts) {
		latestTimestamp = ts;
		//logger.debug("{}: setLatestTimestamp: {}", shardId, latestTimestamp.getTime());
    }
	
	protected synchronized void setLatestTimestamp(BsonDocument document) {
		latestTimestamp = document.getTimestamp("ts");
		//logger.debug("{}: setLatestTimestamp: {}", shardId, latestTimestamp.getTime());
    }
	
	private void processLoop() {
		try {
			timestampFile.update(latestTimestamp);
		} catch (IOException e) {
			logger.error("error updating timestamp file", e);
		}
		
		BsonTimestamp sourceTs = sourceShardClient.getLatestOplogTimestamp(shardId);
		Integer lagSeconds = null;
		
		if (latestTimestamp != null) {
			lagSeconds = sourceTs.getTime() - latestTimestamp.getTime();
		}
		
		//
		
		int queuedTasks = 0;
		if (childQueues != null) {
			for (Map.Entry<Integer, BlockingQueue<OplogQueueEntry>> entry : childQueues.entrySet()) {
				BlockingQueue<OplogQueueEntry> queue = entry.getValue();
				int queueSize = queue.size();
				logger.debug("{} - executor {} - queue size: {}", shardId, entry.getKey(), queueSize);
				queuedTasks += queueSize;
			}
			logger.debug("{} - lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, dupeKey: {}, queuedTasks: {}",
					shardId, lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, duplicateKeyExceptionCount,
					queuedTasks);
			
		} else {
			logger.debug("{} - lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, dupeKey: {}",
					shardId, lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, duplicateKeyExceptionCount);
		}
	}

	@Override
	public void run() {
		try {
			processLoop();
		} catch (Exception e) {
			logger.error("monitor error", e);
		}
	}

	public synchronized void updateStatus(final BulkWriteOutput output) {
		duplicateKeyExceptionCount += output.getDuplicateKeyExceptionCount();
		deletedCount += output.getDeletedCount();
		modifiedCount += output.getModifiedCount();
		insertedCount += output.getInsertedCount();
		upsertedCount += output.getUpsertedCount();
	}

}
