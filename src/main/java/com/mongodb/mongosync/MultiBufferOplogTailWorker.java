package com.mongodb.mongosync;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import com.mongodb.MongoException;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class MultiBufferOplogTailWorker extends AbstractOplogTailWorker implements Runnable {

	Map<String, Map<Integer, List<BsonDocument>>> buffers;
	
	private Map<Integer, ArrayBlockingQueue<Runnable>> workQueues;
	Map<Integer, ThreadPoolExecutor> executors;
	private int numWorkQueues;

	
	public MultiBufferOplogTailWorker(ShardTimestamp shardTimestamp, TimestampFile timestampFile, 
			ShardClient sourceShardClient, ShardClient destShardClient,
			MongoSyncOptions options) throws IOException {
		this.shardId = shardTimestamp.getShardName();
		this.shardTimestamp = shardTimestamp;
		this.sourceShardClient = sourceShardClient;
		this.destShardClient = destShardClient;
		this.options = options;
		this.numWorkQueues = options.getOplogThreads();
		
		workQueues = new HashMap<>(numWorkQueues);
		executors = new HashMap<>(numWorkQueues);
		
		buffers = new HashMap<>();
		for (int i = 0; i < numWorkQueues; i++) {
			ArrayBlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<Runnable>(options.getOplogQueueSize());
			workQueues.put(i, workQueue);
			ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1, 30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
			executors.put(i, pool);
	        //pool.prestartAllCoreThreads();
		}
		
		oplogTailMonitor = new OplogTailMonitor(timestampFile, sourceShardClient, executors);
		this.applyOperationsHelper = new ApplyOperationsHelper(shardId, oplogTailMonitor, destShardClient);
	}
	
	protected Integer addToBuffer(BsonDocument doc) {
		if (currentNs.endsWith(".$cmd")) {
			logger.debug("$cmd: {}", doc);
		}
		BsonValue id = getIdForOperation(doc);
		if (id == null) {
			return null;
		}
		Integer hashKey = getModuloKeyForBsonValue(id);
		
		List<BsonDocument> buffer = null;
		Map<Integer, List<BsonDocument>> bmap = buffers.get(currentNs);
		if (bmap == null) {
			bmap = new HashMap<>();
			buffers.put(currentNs, bmap);
			//buffer = Collections.synchronizedList(new ArrayList<>(options.getBatchSize()));
			buffer = new ArrayList<>(options.getBatchSize());
			bmap.put(hashKey, buffer);
		} else {
			buffer = bmap.get(hashKey);
			if (buffer == null) {
				//buffer = Collections.synchronizedList(new ArrayList<>(options.getBatchSize()));
				buffer = new ArrayList<>(options.getBatchSize());
				bmap.put(hashKey, buffer);
			}
		}
		buffer.add(doc);
		
		if (buffer.size() >= options.getBatchSize()) {
			List<BsonDocument> oldBuffer = buffer;
			buffer = new ArrayList<>(options.getBatchSize());
			bmap.put(hashKey, buffer);
        	flushBuffer(oldBuffer, hashKey);
        }
		return hashKey;
	}
	
	private BsonValue getIdForOperation(BsonDocument operation) throws MongoException {
		String opType = operation.getString("op").getValue();
		switch (opType) {
		case "u":
			BsonDocument o2 = operation.getDocument("o2");
			if (o2 != null) {
				BsonValue id = o2.get("_id");
				if (id != null) {
					return id;
				} else {
					logger.warn("{}: did not find o2._id field for update oplog entry: {}", shardId, operation);
				}
			} else {
				logger.error("{}: did not find o2 field for update oplog entry: {}", shardId, operation);
				return null;
			}
			break;
		case "i":
		case "d":
			BsonDocument oDoc = operation.getDocument("o");
			if (oDoc != null) {
				BsonValue id = oDoc.get("_id");
				if (id != null) {
					return id;
				} else {
					logger.warn("{}: did not find o._id field for insert/delete oplog entry: {}", shardId, operation);
				}
			} else {
				logger.error("{}: did not find o field for insert/delete oplog entry: {}", shardId, operation);
			}
			break;
		default:
			logger.error(String.format("{}: unexpected operation %s; op: %s", shardId, opType, operation.toJson()));
		}
		return null;
	}
	
	private Integer getModuloKeyForBsonValue(BsonValue val) {
		Integer key = null;
		int hash = val.hashCode();
		if (hash >= 0) {
			key = hash % numWorkQueues;
		} else {
			key = -hash % numWorkQueues;
		}
		return key;
	}
	
	private Integer getModuloKeyForNamespace(String ns) {
		Integer key = null;
		int hash = ns.hashCode();
		if (hash >= 0) {
			key = hash % numWorkQueues;
		} else {
			key = -hash % numWorkQueues;
		}
		return key;
	}
	
	protected void flushBuffer(List<BsonDocument> buffer, Integer hashKey) throws MongoException {
		
		if (buffer.size() > 0) {
			applyOperations(currentNs, buffer, hashKey);
		}
	}
	
	private void applyOperations(String ns, List<BsonDocument> operations, Integer hashKey) throws MongoException {
		//Integer key = getModuloKeyForNamespace(ns);
		ApplyOperationsTask task = new ApplyOperationsTask(applyOperationsHelper, operations);
		ThreadPoolExecutor executor = executors.get(hashKey);
		try {
			executor.execute(task);
		} catch (RejectedExecutionException ree) {
			logger.error("{}: error executing task", shardId, ree);
		}
		
	}
	
	@Override
	protected synchronized void stop() {
		logger.debug("{}: OplogTailWorker got stop signal", shardId);
		this.shutdown = true;
		
		
		try {
			flushBuffers();
		} catch (MongoException | IOException e1) {
			logger.error("error flushing buffers", e1);
		}
		logger.debug("{}: buffers flushed", shardId);
		
		int i = 0;
		for (ThreadPoolExecutor executor : executors.values()) {
			try {
				logger.debug("{}: starting shutdown of executor {}", shardId, i++);
		        executor.shutdown();
		        executor.awaitTermination(60, TimeUnit.SECONDS);
		      } catch (InterruptedException e) {
		        logger.error("Timed out waiting for executor to complete", e);
		      }
		}
		logger.debug("{}: executors shutdown", shardId);
		      
		    
	}

	@Override
	protected synchronized void flushBuffers() throws MongoException, IOException {
		
		for (Map.Entry<String, Map<Integer, List<BsonDocument>>> buffersEntry : buffers.entrySet()) {
			Map<Integer, List<BsonDocument>> bmap = buffersEntry.getValue();
			String ns = buffersEntry.getKey();
			if (bmap.size() > 0) {
				
				for (Map.Entry<Integer, List<BsonDocument>> entry : bmap.entrySet()) {
					List<BsonDocument> buffer = entry.getValue();
					applyOperations(ns, buffer, entry.getKey());
					buffer.clear();
				}
			}
		}
	}
}
