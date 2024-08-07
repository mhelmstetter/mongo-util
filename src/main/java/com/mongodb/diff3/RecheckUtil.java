package com.mongodb.diff3;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Updates.pullAll;
import static com.mongodb.ErrorCategory.DUPLICATE_KEY;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.ErrorCategory;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoWriteException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.DiffUtils;

public class RecheckUtil {
	
	private static Logger logger = LoggerFactory.getLogger(RecheckUtil.class);

	private final ShardClient sourceShardClient;
	private final ShardClient destShardClient;
	
	private final MongoClient mongoClient;
	private final MongoDatabase db;
    private final MongoCollection<BsonDocument> coll;

	private final DiffConfiguration config;
	
	private ChunkManager chunkManager;
	private Bson chunkQuery;

	public RecheckUtil(DiffConfiguration config) {

        this.config = config;
        
        if (config.isFiltered()) {
        	chunkManager = new ChunkManager(config);
    		Bson query = chunkManager.initializeSourceChunkQuery();
    		this.chunkQuery = and(query, eq("status", "RETRYING"));
        } else {
        	this.chunkQuery = eq("status", "RETRYING");
        }
        
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
	
	public void recheck() {
		
		List<BsonDocument> failedChunks = new ArrayList<>();
		
		if (chunkQuery != null) {
			coll.find(chunkQuery).into(failedChunks);
		} else {
			failedChunks = coll.find().into(failedChunks);
		}
		
		logger.debug("{} failed chunks found, chunkQuery: {}", failedChunks.size(), chunkQuery);
		
		
		for (BsonDocument failed : failedChunks) {
			
			Namespace ns = new Namespace(failed.getString("ns").getValue());
			MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
			MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
			
			MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
			MongoCollection<RawBsonDocument> destColl = destDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
			
			recheck(failed, "mismatches", ns, sourceColl, destColl);
			recheck(failed, "srcOnly", ns, sourceColl, destColl);
			recheck(failed, "destOnly", ns, sourceColl, destColl);
			
		}
		
	}
	
	private void recheck(BsonDocument failed, String failedKey, Namespace ns, MongoCollection<RawBsonDocument> sourceColl, MongoCollection<RawBsonDocument> destColl) {
		
		BsonArray failures = failed.getArray(failedKey);
		
		if (failures.size() == 0) {
			return;
		}
		
		logger.debug("{} - recheck {}, failureCount: {}", ns, failedKey, failures.size());
		
		List<BsonValue> passedKeys = new ArrayList<>();
		
		for (BsonValue m : failures) {
			
			BsonValue key = null;
			
			if (m instanceof BsonDocument) {
				BsonDocument d = (BsonDocument)m;
				
				if (d.containsKey("key")) {
					key = d.get("key");
				} else {
					key = d;
				}
			} else {
				key = m;
			}
			
			Bson query = eq("_id", key);
			
			RawBsonDocument sourceDoc = null;
			RawBsonDocument destDoc = null;
			
			Iterator<RawBsonDocument> sourceDocs = sourceColl.find(query).iterator();
			if (sourceDocs.hasNext()) {
				sourceDoc = sourceDocs.next();
			} 

			if (sourceDocs.hasNext()) {
				logger.error("{}: duplicate source documents found with same key: {}", ns, key);
			}
			
			Iterator<RawBsonDocument> destDocs = destColl.find(query).iterator();
			if (destDocs.hasNext()) {
				destDoc = destDocs.next();
			} 

			if (destDocs.hasNext()) {
				logger.error("{}: duplicate dest documents found with same key: {}", ns, key);
			}
			
			if (sourceDoc == null && destDoc == null) {
				logger.debug("{}: both documents are null, key: {}", ns, key);
				passedKeys.add(m);
				continue;
			}
			
			if (sourceDoc == null && destDoc != null) {
				logger.error("{}: source doc does not exist: {}", ns, key);
				if (config.isArchive()) {
					archive(ns, "diffDestOnly", destDoc);
				}
				if (config.isArchiveAndDeleteDestOnly()) {
					archiveAndDeleteDestOnly(ns, destDoc, destColl, key);
				}
				continue;
			}
			
			if (destDoc == null) {
				logger.error("{}: dest doc does not exist: {}, query: {}", ns, key, query);
				if (config.isArchive()) {
					archive(ns, "diffSourceOnly", sourceDoc);
				}
				if (config.isSyncMismatches()) {
					try {
						syncSourceOnly(sourceDoc, destColl);
					} catch (MongoWriteException ex) {
						
						if(ex.getError().getCategory() == ErrorCategory.DUPLICATE_KEY) {
					        logger.error("duplicate key error: {}", ex);
					    } 
					}
					
				}
				continue;
			}
			
			boolean pass = compareDocuments(ns, sourceDoc, destDoc);
			
			if (pass) {
				passedKeys.add(m);
			} else {
				logger.debug("{}: doc mismatch, key: {}", ns, key);
				if (config.isArchive()) {
					archive(ns, "diffSourceMismatch", sourceDoc);
					archive(ns, "diffDestMismatch", destDoc);
				}
				if (config.isSyncMismatches()) {
					syncMismatch(sourceDoc, destColl, key);
				}
			}
		}
		if (passedKeys.size() > 0) {
			Bson filter = eq("_id", failed.get("_id"));
			Bson update = pullAll(failedKey, passedKeys);
			UpdateResult result = coll.updateOne(filter, update);
			logger.debug("Status update result: {}", result);
		}

	}
	
	private void syncSourceOnly(RawBsonDocument srcDoc, MongoCollection<RawBsonDocument> destColl) {
		destColl.insertOne(srcDoc);
	}
	
	private void syncMismatch(RawBsonDocument srcDoc, MongoCollection<RawBsonDocument> destColl, BsonValue key) {
		destColl.replaceOne(eq("_id", key), srcDoc);
	}
	
	private void archiveAndDeleteDestOnly(Namespace ns, RawBsonDocument destDoc, MongoCollection<RawBsonDocument> destColl, BsonValue key) {
		MongoDatabase db = destShardClient.getMongoClient().getDatabase("rollbackArchive");
		MongoCollection<RawBsonDocument> coll = db.getCollection(ns.getNamespace(), RawBsonDocument.class);
		coll.insertOne(destDoc);
		destColl.deleteOne(eq("_id", key));
	}
	
	private void archive(Namespace ns, String archiveDb, RawBsonDocument doc) {
		MongoDatabase db = destShardClient.getMongoClient().getDatabase(archiveDb);
		MongoCollection<RawBsonDocument> coll = db.getCollection(ns.getNamespace(), RawBsonDocument.class);
		// 
        
		try {
			coll.insertOne(doc);
		} catch (MongoWriteException mwe) {
			if (mwe.getError().getCategory() != DUPLICATE_KEY) {
				logger.error("archive error", mwe);
			}
		}
		
	}
	
	private boolean compareDocuments(Namespace ns, RawBsonDocument sourceDoc, RawBsonDocument destDoc) {
		byte[] sourceBytes = sourceDoc.getByteBuffer().array();
		byte[] destBytes = destDoc.getByteBuffer().array();
		
		Object id = sourceDoc.get("_id");
		
		if (sourceBytes.length == destBytes.length) {
			if (DiffUtils.compareHashes(sourceBytes, destBytes)) {
				//logger.debug("hashes match");
				return true;
			} else {
				if (sourceDoc.equals(destDoc)) {
					logger.warn(String.format("%s - docs equal, but hash mismatch, id: %s", ns, id));
					return true;
				} else {
					logger.error(String.format("%s - doc hash mismatch, id: %s", ns, id));
					return false;
				}
			}
		} else {
			
			boolean docCheck = DiffUtils.compareDocuments(ns.getNamespace(), sourceDoc, destDoc);
			if (docCheck) {
				logger.debug("{} - docs are equivalent, id: {}, sourceBytes: {}, destBytes: {}", ns, id, sourceBytes.length, destBytes.length);
				return true;
			} else {
				logger.debug("{} - docs are not equivalent, id: {}, sourceBytes: {}, destBytes: {}", ns, id, sourceBytes.length, destBytes.length);
				return false;
			}
		}
	}

}
