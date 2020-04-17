package com.mongodb.diffutil;

import java.util.Set;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

public class ShardedDupeIdFinderWorker implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ShardedDupeIdFinderWorker.class);

	private MongoCollection<RawBsonDocument> collection;
	private MongoClient client;
	Set<BsonValue> idsSet;

	Document sort = new Document("_id", 1);
	private String shardName;

	public ShardedDupeIdFinderWorker(com.mongodb.client.MongoClient sourceClient,
			MongoCollection<RawBsonDocument> collection, Set<BsonValue> syncSet, String shardName) {
		this.collection = collection;
		this.client = sourceClient;
		this.idsSet = syncSet;
		this.shardName = shardName;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		long count = 0;
		MongoCursor<RawBsonDocument> sourceCursor = null;
		try {
			sourceCursor = collection.find().projection(sort).iterator();
			while (sourceCursor.hasNext()) {
				count++;
				RawBsonDocument doc = sourceCursor.next();
				BsonValue id = null;
				try {
					id = doc.get("_id");
				} catch (Exception e) {
					logger.debug(String.format("%s: %s - Error reading doc id, count: %s, error: %s",
							shardName, collection.getNamespace(), count, e));
					continue;
				}
				boolean newEntry = idsSet.add(id);
				if (!newEntry) {
					logger.debug(String.format("%s: dupe key for %s", shardName, id));
				}
				
				if (count % 1000000 == 0) {
					logger.debug(String.format("%s: read %s", shardName, count));
				}
			}

		} finally {
			sourceCursor.close();

		}
		long end = System.currentTimeMillis();
		Double dur = (end - start) / 1000.0;
		logger.debug(
				String.format("Done validating %s, %s documents in %f seconds", collection.getNamespace(), count, dur));

	}

}
