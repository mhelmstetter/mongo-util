package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class DupeIdTask implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(DupeIdTask.class);
	private final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
	
	private final static Bson sort = eq("_id", 1);
	private final static Bson proj = include("_id");
	
	private MongoCollection<RawBsonDocument> collection;
	private MongoDatabase archiveDb;
	private String base;
	private List<BsonValue> dupesBatch;
	private Map<String, List<WriteModel<RawBsonDocument>>> writeModelsMap = new HashMap<>();
	
	BsonValue startId;
	BsonValue endId;
	

	private final static int BATCH_SIZE = 1000;

	public DupeIdTask(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, BsonValue startId, BsonValue endId) {
		this.collection = collection;
		this.archiveDb = archiveDb;
		this.base = collection.getNamespace().getFullName();
		this.startId = startId;
		this.endId = endId;
	}

	@Override
	public Integer call() throws Exception {
		
		MongoCursor<RawBsonDocument> cursor = null;
       
        long count = 0;
        long dupeCount = 0;
        
        BsonValue lastId = null;
        
		try {
			
			
    		Bson query = and(gte("_id", startId), lt("_id", endId));
    		cursor = collection.find(query).projection(proj).sort(sort).iterator();

			while (cursor.hasNext()) {
                count++;
                RawBsonDocument doc = cursor.next();
                BsonValue id = null;
                try {
                    id = doc.get("_id");
                } catch (Exception e) {
                    logger.warn(String.format("%s - Error reading doc id, count: %s, error: %s",
                            collection.getNamespace(), count, e));
                    continue;
                }
                
                if (id.equals(lastId)) {
                	handleDupes(doc);
                	dupeCount++;
                	logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                }
                lastId = id;
                //lastDocument = doc;
            }
			
		} catch (Exception e) {
			logger.error("call error", e);
		} finally {
			cursor.close();
            flushAll();
		}
		return null;
	}
	
	private void handleDupes(RawBsonDocument doc) {
    	if (archiveDb != null) {
    		
    		dupesBatch.add(doc.get("_id"));
    		if (dupesBatch.size() >= BATCH_SIZE) {
    			
    			Bson query = in("_id", dupesBatch);
        		int d = 1;
        		MongoCursor<RawBsonDocument> cursor = collection.find(query).sort(sort).iterator();
        		
        		BsonValue lastId = null;
        		
        		while (cursor.hasNext()) {
                    RawBsonDocument fullDoc = cursor.next();
                    BsonValue id = null;
                    try {
                        id = doc.get("_id");
                    } catch (Exception e) {
                        logger.warn(String.format("%s - Error reading doc id, fullDoc: %s, error: %s",
                                collection.getNamespace(), fullDoc, e));
                        continue;
                    }
                    
                    if (id.equals(lastId)) {
                    	handleDupes(doc);
                    	logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                    	
                    	String collName = String.format("%s_%s", base, d++);
        				insert(fullDoc, collName);
                    } else {
                    	d = 1;
                    }
                    lastId = id;
        		}
        		
        		dupesBatch.clear();
        		
    		}
    	}
    	
    }

	private void flushDupes(List<RawBsonDocument> dupesBuffer) {
		if (dupesBuffer.size() > 1) {
			int i = 1;
			for (RawBsonDocument dupe : dupesBuffer) {
				String collName = String.format("%s_%s", base, i++);
				insert(dupe, collName);
			}
		} else if (dupesBuffer.size() == 1) {
			BsonValue id = dupesBuffer.get(0).get("_id");
			logger.warn("duplicate from manifest not found as duplicate for _id: {}", id);
		}
		dupesBuffer.clear();
	}

	private void insert(RawBsonDocument fullDoc, String collName) {
		WriteModel<RawBsonDocument> model = new InsertOneModel<>(fullDoc);

		List<WriteModel<RawBsonDocument>> writeModels = writeModelsMap.get(collName);
		if (writeModels == null) {
			writeModels = new ArrayList<>(BATCH_SIZE);
			writeModelsMap.put(collName, writeModels);
		}

		writeModels.add(model);
	}

	private void flushInternal(String collName, List<WriteModel<RawBsonDocument>> writeModels) {
		if (writeModels.size() == 0) {
			return;
		}
		BulkWriteResult bulkWriteResult = null;
		try {
			MongoCollection<RawBsonDocument> c1 = archiveDb.getCollection(collName, RawBsonDocument.class);
			bulkWriteResult = c1.bulkWrite(writeModels, bulkWriteOptions);
		} catch (MongoBulkWriteException err) {
			// List<BulkWriteError> errors = err.getWriteErrors();
			bulkWriteResult = err.getWriteResult();
			logger.warn("bulk write errors, insertedCount: {}, errorCount: {}", bulkWriteResult.getInsertedCount(),
					err.getWriteErrors().size());
			logger.error("bulk write error", err);
		} catch (Exception ex) {
			logger.error("{} unknown error: {}", ex.getMessage(), ex);
		} finally {
			writeModels.clear();
		}
	}

	private void flushAll() {

		for (Map.Entry<String, List<WriteModel<RawBsonDocument>>> entry : writeModelsMap.entrySet()) {
			String collName = entry.getKey();
			List<WriteModel<RawBsonDocument>> writeModels = entry.getValue();
			flushInternal(collName, writeModels);
		}

	}

}
