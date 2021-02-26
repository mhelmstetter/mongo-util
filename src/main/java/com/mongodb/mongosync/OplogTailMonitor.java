package com.mongodb.mongosync;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

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
    private long failedOpsCount;
	
	BsonTimestamp latestTimestamp;
	
	private TimestampFile timestampFile;
	
	private ShardClient sourceShardClient;
	//private ClientSession sourceSession;
	private String shardId;
	
	private Map<Integer, ArrayBlockingQueue<BsonDocument>> childQueues;
	
	public OplogTailMonitor(TimestampFile timestampFile, ShardClient sourceShardClient, Map<Integer, ArrayBlockingQueue<BsonDocument>> childQueues) {
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

	@Override
	public void run() {
		try {
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
				for (Map.Entry<Integer, ArrayBlockingQueue<BsonDocument>> entry : childQueues.entrySet()) {
					ArrayBlockingQueue<BsonDocument> queue = entry.getValue();
					int queueSize = queue.size();
					logger.debug("{} - pool {} - queue size: {}", shardId, entry.getKey(), queueSize);
					queuedTasks += queueSize;
				}
				logger.debug("{} - lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, failed: {}, dupeKey: {}, queuedTasks: {}",
						shardId, lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, failedOpsCount, duplicateKeyExceptionCount,
						queuedTasks);
				
			} else {
				logger.debug("{} - lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, failed: {}, dupeKey: {}",
						shardId, lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, failedOpsCount, duplicateKeyExceptionCount);
			}
			
		} catch (Exception e) {
			logger.error("monitor error", e);
		}
	}

	public synchronized void updateStatus(BulkWriteOutput output) {
		duplicateKeyExceptionCount += output.getDuplicateKeyExceptionCount();
		deletedCount += output.getDeletedCount();
		modifiedCount += output.getModifiedCount();
		insertedCount += output.getInsertedCount();
		upsertedCount += output.getUpsertedCount();
		failedOpsCount += output.getFailedOps().size();
	}

}
