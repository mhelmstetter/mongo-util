package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.in;

import java.util.Date;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Namespace;
import com.mongodb.util.DiffUtils;


public class DiffTask implements Callable<DiffResult> {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogTailingDiffUtil.class);
	
	private final static Document SORT_ID = new Document("_id", 1);
	
	private Namespace ns;
	private Set<Object> buffer;
    private MongoClient sourceMongoClient;
    private MongoClient destMongoClient;
    
    private String sourceShardId;
    private long id;

	public DiffTask(Set<Object> buffer, String sourceShardId, Namespace ns, MongoClient sourceClient, MongoClient destClient, long id) {
		this.buffer = buffer;
		this.ns = ns;
		this.sourceMongoClient = sourceClient;
		this.destMongoClient = destClient;
		this.sourceShardId = sourceShardId;
		this.id = id;
	}

	@Override
	public DiffResult call() throws Exception {
		//logger.debug("DiffTask.call()");
		
		DiffResult result = new DiffResult(sourceShardId);
		
		Bson filter = in("_id", buffer);
		
		MongoDatabase sourceDb = sourceMongoClient.getDatabase(ns.getDatabaseName());
		MongoDatabase destDb = destMongoClient.getDatabase(ns.getDatabaseName());
		MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
		MongoCollection<RawBsonDocument> destColl = destDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);

		MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find(filter).sort(SORT_ID).iterator();
		MongoCursor<RawBsonDocument> destCursor = destColl.find(filter).sort(SORT_ID).iterator();

		RawBsonDocument sourceDoc = null;
		RawBsonDocument destDoc = null;
		byte[] sourceBytes = null;
		byte[] destBytes = null;
		
		long lastReport = System.currentTimeMillis();
		while (sourceCursor.hasNext()) {
			sourceDoc = sourceCursor.next();
			Object sourceId = sourceDoc.get("_id");
			if (destCursor.hasNext()) {
				destDoc = destCursor.next();
			} else {
				logger.error(String.format("%s - destCursor exhausted, doc %s missing", ns, sourceId));
				result.incrementMissing();
				continue;
			}
			sourceBytes = sourceDoc.getByteBuffer().array();
			destBytes = destDoc.getByteBuffer().array();
			if (sourceBytes.length == destBytes.length) {
				if (!DiffUtils.compareHashes(sourceBytes, destBytes)) {
					Object id = sourceDoc.get("_id");

					if (sourceDoc.equals(destDoc)) {
						logger.error(String.format("%s - docs equal, but hash mismatch, id: %s", ns, id));
						result.incrementKeysMisordered();
					} else {
						logger.error(String.format("%s - doc hash mismatch, id: %s", ns, id));
						result.incrementHashMismatched();
					}

				} else {
					//logger.debug("Match! " + sourceId);
					result.incrementMatches();
				}
			} else {
				logger.debug("Doc sizes not equal, id: " + sourceId);
				boolean xx = DiffUtils.compareDocuments(ns.getNamespace(), sourceDoc, destDoc);
				result.incrementHashMismatched();
			}
			result.incrementTotal();
		}
		//logger.debug(String.format("%s - call complete, id: %s", sourceShardId, id));
		return result;
	}

}
