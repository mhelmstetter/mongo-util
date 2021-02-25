package com.mongodb.mongosync;

import java.util.List;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApplyOperationsTask implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(ApplyOperationsTask.class);
	
	private List<BsonDocument> operations;
	private ApplyOperationsHelper helper;
	
	
	public ApplyOperationsTask(ApplyOperationsHelper helper, List<BsonDocument> operations) {
		this.helper = helper;
		this.operations = operations;
	}

	@Override
	public void run() {
		try {
			helper.applyOperations(operations);
			operations = null;
		} catch (Exception e) {
			logger.error("ApplyOperationsTask error", e);
		}
	}
	
	

}
