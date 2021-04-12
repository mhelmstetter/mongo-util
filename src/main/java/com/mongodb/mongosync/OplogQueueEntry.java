package com.mongodb.mongosync;

import org.bson.BsonDocument;
import org.bson.BsonValue;

public class OplogQueueEntry {
	
	public BsonDocument doc;
	public BsonValue id;
	
	public OplogQueueEntry(BsonDocument doc, BsonValue id) {
		this.doc = doc;
		this.id = id;
	}

}
