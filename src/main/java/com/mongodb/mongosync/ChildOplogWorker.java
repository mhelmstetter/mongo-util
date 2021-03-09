package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;

public class ChildOplogWorker implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(ChildOplogWorker.class);
	
	private String shardId;
	private BlockingQueue<OplogQueueEntry> workQueue;
	private ApplyOperationsHelper applyOperationsHelper;
	private OplogTailMonitor oplogTailMonitor;
	
	private boolean shutdown = false;
	
	private Map<String, OplogBatch> oplogBatches;
	
	private BsonTimestamp lastTimestamp = null;
	
	private MongoSyncOptions options;
	
	private Map<String, LookupTransformer> lookupTransformers;
	
	public ChildOplogWorker(String shardId, BlockingQueue<OplogQueueEntry> workQueue, 
			ApplyOperationsHelper applyOperationsHelper, OplogTailMonitor oplogTailMonitor, MongoSyncOptions options) {
		this.shardId = shardId;
		this.workQueue = workQueue;
		this.applyOperationsHelper = applyOperationsHelper;
		this.oplogTailMonitor = oplogTailMonitor;
		this.oplogBatches = new HashMap<>();
		this.options = options;
		initializeTransformers();
	}
	
	private void initializeTransformers() {
		for (Map.Entry<String, String> entry : options.getTransformersMap().entrySet()) {
			if (lookupTransformers == null) {
				lookupTransformers = new HashMap<>();
			}
			MongoClient sourceClient = options.getSourceShardClient().getMongoClient();
			Namespace ns = new Namespace(entry.getKey());
			LookupTransformer transformer = new LookupTransformer(sourceClient, ns.getDatabaseName(), ns.getCollectionName(), entry.getValue());
			lookupTransformers.put(entry.getKey(), transformer);
		}
	}
	
	public void stop() {
		shutdown = true;
	}
	
	private void flush(int minThreshold) {
		//synchronized (writeModelsMap) {
			for (Map.Entry<String, OplogBatch> entry : oplogBatches.entrySet()) {
				Namespace ns = new Namespace(entry.getKey());
				flush(minThreshold, ns, entry.getValue());
			}
		//}
			
	}
	
	private void flush(int minThreshold, Namespace ns, OplogBatch oplogBatch) {
		BulkWriteOutput output = null;
		//synchronized(models) {
			if (oplogBatch.size() > minThreshold) {
				output = applyOperationsHelper.applyBulkWriteModelsOnCollection(ns, oplogBatch);
				oplogBatch.clear();
			}
		//}
		if (output != null) {
			oplogTailMonitor.updateStatus(output);
			oplogTailMonitor.setLatestTimestamp(new BsonTimestamp(lastTimestamp.getValue()));
		}
	}
	

	@Override
	public void run() {
		
		while (!shutdown) {
			
			try {
				OplogQueueEntry currentQueueEntry = null;
				
				try {
					currentQueueEntry = workQueue.poll(5, TimeUnit.SECONDS);
					//currentDocument = workQueue.take();
				} catch (InterruptedException e) {
					if (shutdown) {
						logger.debug("{}: interruped, breaking", shardId);
						break;
					}
				}
				
				if (currentQueueEntry == null) {
					flush(options.getOplogBatchSize());
					continue;
				}
				BsonDocument currentDocument = currentQueueEntry.doc;
				
				String ns = currentDocument.getString("ns").getValue();
				Namespace namespace = new Namespace(ns);
				String op = currentDocument.getString("op").getValue();
				if (op.equals("c") || namespace.getCollectionName().equals("system.indexes")) {
					continue;
				} 
				
				OplogBatch batch = oplogBatches.get(ns);
				if (batch == null) {
					batch = new OplogBatch(options.getOplogBatchSize());
					oplogBatches.put(ns, batch);
				}
				
				if (lookupTransformers != null && op.equals("u")) {
					LookupTransformer tran = lookupTransformers.get(ns);
					if (tran != null) {
						BsonDocument updateQuery = currentDocument.getDocument("o2");
						ObjectId id = updateQuery.getObjectId("_id").getValue();
						BsonValue lookupValue = null;
						try {
							lookupValue = tran.lookup(id);
							updateQuery.put(tran.getLookupValueKey(), lookupValue);
						} catch (Exception e) {
							logger.warn("ChildOplogWorker exception executing transformation lookup", e);
						}
						
						//BsonDocument updateQuery = currentDocument.getDocument("o2");
						
					}
					
				}
				
				WriteModel<BsonDocument> model = ApplyOperationsHelper.getWriteModelForOperation(currentDocument);
				if (model != null) {
					//synchronized(oplogBatches) {
						batch.addWriteModel(model, currentQueueEntry.id);
					//}
				} else {
					// if the command is $cmd for create index or create collection, there would not
					// be any write model.
					logger.warn("{}: ignoring oplog entry. could not convert the document to model. Given document is {}", 
							shardId, currentDocument.toJson());
				}
				lastTimestamp = currentDocument.getTimestamp("ts");
				
				flush(options.getOplogBatchSize(), namespace, batch);
				
			} catch (Exception e) {
				logger.error("{}: ChildOplogWorker error", shardId, e);
				
			}
			
		}
		logger.debug("{}: child flush", shardId);
		flush(0);
		
	}

}
