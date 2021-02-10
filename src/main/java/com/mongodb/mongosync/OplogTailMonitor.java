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
	
	public OplogTailMonitor(TimestampFile timestampFile, ShardClient sourceShardClient) {
		this.timestampFile = timestampFile;
		this.sourceShardClient = sourceShardClient;
		this.shardId = timestampFile.getShardId();
		//this.sourceSession = sourceShardClient.getShardMongoClient(shardId).startSession();
	}
	
	protected void setLatestTimestamp(BsonDocument document) throws IOException {
		latestTimestamp = document.getTimestamp("ts");
    }

	@Override
	public void run() {
		System.out.println(shardId + " run()");
		try {
			timestampFile.update(latestTimestamp);
		} catch (IOException e) {
			logger.error("error updating timestamp file", e);
		}
		
		BsonTimestamp sourceTs = sourceShardClient.getLatestOplogTimestamp(shardId);
		long delta = sourceTs.getValue() - latestTimestamp.getValue();
		long lagSeconds = delta / 1000;
		System.out.println("lagSeconds: " + lagSeconds);
		
		logger.debug("lagSeconds: {}, inserted: {}, modified: {}, upserted: {}, deleted: {}, failed: {}, dupeKey: {}",
				lagSeconds, insertedCount, modifiedCount, upsertedCount, deletedCount, failedOpsCount, duplicateKeyExceptionCount);
		
		
		
		
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
