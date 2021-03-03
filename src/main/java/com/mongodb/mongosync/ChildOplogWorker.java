package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;

public class ChildOplogWorker implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(ChildOplogWorker.class);
	
	private String shardId;
	private ArrayBlockingQueue<BsonDocument> workQueue;
	private ApplyOperationsHelper applyOperationsHelper;
	private OplogTailMonitor oplogTailMonitor;
	
	private boolean shutdown = false;
	
	private Map<String, List<WriteModel<BsonDocument>>> writeModelsMap;
	
	private BsonTimestamp lastTimestamp = null;
	
	public ChildOplogWorker(String shardId, ArrayBlockingQueue<BsonDocument> workQueue, 
			ApplyOperationsHelper applyOperationsHelper, OplogTailMonitor oplogTailMonitor) {
		this.shardId = shardId;
		this.workQueue = workQueue;
		this.applyOperationsHelper = applyOperationsHelper;
		this.oplogTailMonitor = oplogTailMonitor;
		this.writeModelsMap = new HashMap<>();
	}
	
	public void stop() {
		shutdown = true;
	}
	
	private void flush() {
		for (Map.Entry<String, List<WriteModel<BsonDocument>>> entry : writeModelsMap.entrySet()) {
			String ns = entry.getKey();
			List<WriteModel<BsonDocument>> models = entry.getValue();
			if (models.size() > 0) {
				Namespace namespace = new Namespace(ns);
				BulkWriteOutput output = applyOperationsHelper.applyBulkWriteModelsOnCollection(namespace, models);
				models.clear();
				oplogTailMonitor.updateStatus(output);
				oplogTailMonitor.setLatestTimestamp(lastTimestamp);
			}
		}
	}

	@Override
	public void run() {
		
		while (!shutdown) {
			
			try {
				BsonDocument currentDocument = null;
				try {
					currentDocument = workQueue.poll(10, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					if (shutdown) {
						logger.debug("{}: interruped, breaking", shardId);
						break;
					}
				}
				
				if (currentDocument == null) {
					flush();
					continue;
				}
				
				String ns = currentDocument.getString("ns").getValue();
				Namespace namespace = new Namespace(ns);
				String op = currentDocument.getString("op").getValue();
				if (op.equals("c")) {
					continue;
				} 
				
				List<WriteModel<BsonDocument>> models = writeModelsMap.get(ns);
				if (models == null) {
					models = new ArrayList<>(5000);
					writeModelsMap.put(ns, models);
				}
				
				WriteModel<BsonDocument> model = ApplyOperationsHelper.getWriteModelForOperation(currentDocument);
				if (model != null) {
					models.add(model);
				} else {
					// if the command is $cmd for create index or create collection, there would not
					// be any write model.
					logger.warn("{}: ignoring oplog entry. could not convert the document to model. Given document is {}", 
							shardId, currentDocument.toJson());
				}
				lastTimestamp = currentDocument.getTimestamp("ts");
				
				if (models.size() >= 5000) {
					BulkWriteOutput output = applyOperationsHelper.applyBulkWriteModelsOnCollection(namespace, models);
					models.clear();
					oplogTailMonitor.updateStatus(output);
					oplogTailMonitor.setLatestTimestamp(lastTimestamp);
				}				
			} catch (Exception e) {
				logger.error("{}: ChildOplogWorker error", shardId, e);
				
			}
			
		}
		logger.debug("{}: child flush", shardId);
		flush();
		
	}

}
