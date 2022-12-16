package com.mongodb.diff3;

import static com.mongodb.client.model.Filters.eq;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.diff3.DiffSummary.DiffStatus;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.DiffUtils;

public class RetryUtil {
	
	private static Logger logger = LoggerFactory.getLogger(RetryUtil.class);

	private final ShardClient sourceShardClient;
	private final ShardClient destShardClient;
	
	private final MongoClient mongoClient;
	private final MongoDatabase db;
    private final MongoCollection<BsonDocument> coll;

	private final DiffConfiguration config;

	public RetryUtil(DiffConfiguration config) {

        this.config = config;


        sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
        destShardClient = new ShardClient("dest", config.getDestClusterUri());
        
        
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
				.applyConnectionString(new ConnectionString(config.getStatusDbUri()))
				.uuidRepresentation(UuidRepresentation.STANDARD)
				.build();
		mongoClient = MongoClients.create(mongoClientSettings);
		
		this.db = this.mongoClient.getDatabase(config.getStatusDbName());
        this.coll = this.db.getCollection(config.getStatusDbCollName(), BsonDocument.class);


        sourceShardClient.init();
        destShardClient.init();
	}
	
	public void retry() {
		
		FindIterable<BsonDocument> failedChunks = coll.find(eq("status", DiffStatus.FAILED));
		
		for (BsonDocument failed : failedChunks) {
			
			
			
			String db = failed.getString("db").getValue();
			String coll = failed.getString("coll").getValue();
			Namespace ns = new Namespace(db, coll);
			MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(db);
			MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(coll, RawBsonDocument.class);
			
			MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(db);
			MongoCollection<RawBsonDocument> destColl = destDb.getCollection(coll, RawBsonDocument.class);
			
			BsonArray mismatches = failed.getArray("mismatches");
			for (BsonValue m : mismatches) {
				BsonDocument d = (BsonDocument)m;
				
				BsonDocument key = null;
				if (d.containsKey("key")) {
					key = d.getDocument("key");
				} else {
					key = d;
				}
				
				FindIterable<RawBsonDocument> sourceDocs = sourceColl.find(key);
				RawBsonDocument sourceDoc = sourceDocs.iterator().next();
				if (sourceDocs.iterator().hasNext()) {
					logger.error("{}: duplicate source documents found with same key: {}", ns, key);
				}
				
				FindIterable<RawBsonDocument> destDocs = destColl.find(key);
				RawBsonDocument destDoc = destDocs.iterator().next();
				if (destDocs.iterator().hasNext()) {
					logger.error("{}: duplicate dest documents found with same key: {}", ns, key);
				}
				
				compareDocuments(ns, sourceDoc, destDoc);
				
			}
		}
		
	}
	
	private void compareDocuments(Namespace ns, RawBsonDocument sourceDoc, RawBsonDocument destDoc) {
		byte[] sourceBytes = sourceDoc.getByteBuffer().array();
		byte[] destBytes = destDoc.getByteBuffer().array();
		
		Object id = sourceDoc.get("_id");
		
		if (sourceBytes.length == destBytes.length) {
			if (!DiffUtils.compareHashes(sourceBytes, destBytes)) {
				

				if (sourceDoc.equals(destDoc)) {
					logger.error(String.format("%s - docs equal, but hash mismatch, id: %s", ns, id));
				} else {
					logger.error(String.format("%s - doc hash mismatch, id: %s", ns, id));
				}

			} else {
				logger.debug(String.format("%s - hashes match, id: %s", ns, id));
			}
		} else {
			logger.debug("Doc sizes not equal, id: {}", id);
		}
	}

}
