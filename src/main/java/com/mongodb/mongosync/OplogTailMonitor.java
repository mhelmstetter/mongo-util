package com.mongodb.mongosync;

import java.io.IOException;

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
	
	OplogTailWorker worker;
	
	public OplogTailMonitor(OplogTailWorker worker, TimestampFile timestampFile, ShardClient sourceShardClient) {
		this.timestampFile = timestampFile;
		this.sourceShardClient = sourceShardClient;
		this.shardId = timestampFile.getShardId();
		this.worker = worker;
		//this.sourceSession = sourceShardClient.getShardMongoClient(shardId).startSession();
	}
	
	protected void setLatestTimestamp(BsonTimestamp ts) throws IOException {
		latestTimestamp = ts;
		//logger.debug("{}: setLatestTimestamp: {}", shardId, latestTimestamp.getTime());
    }
	
	protected void setLatestTimestamp(BsonDocument document) throws IOException {
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
			
			logger.debug("{} - lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, failed: {}, dupeKey: {}",
					shardId, lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, failedOpsCount, duplicateKeyExceptionCount);
			
		} catch (Exception e) {
			logger.error("monitor error", e);
		}
	}

	public void updateStatus(BulkWriteOutput output) {
		duplicateKeyExceptionCount += output.getDuplicateKeyExceptionCount();
		deletedCount += output.getDeletedCount();
		modifiedCount += output.getModifiedCount();
		insertedCount += output.getInsertedCount();
		upsertedCount += output.getUpsertedCount();
		failedOpsCount += output.getFailedOps().size();
	}

}
