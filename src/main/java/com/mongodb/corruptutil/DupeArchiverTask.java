package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

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

public class DupeArchiverTask implements Callable<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(DupeArchiverTask.class);
	
	private final static int BATCH_SIZE = 10;
	private final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
	Bson sort = eq("_id", 1);

	List<BsonValue> dupesBatch;

	private MongoCollection<RawBsonDocument> collection;

	private MongoDatabase archiveDb;
	
	private String base;
	private Map<String, List<WriteModel<RawBsonDocument>>> writeModelsMap = new HashMap<>();
	

	public DupeArchiverTask(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, List<BsonValue> dupesBatch) {
        this.collection = collection;
        this.archiveDb = archiveDb;
        this.base = collection.getNamespace().getFullName();
        this.dupesBatch = dupesBatch;
    }

	@Override
	public Integer call() throws Exception {
		try {
			Bson query = in("_id", dupesBatch);
    		int d = 1;
    		MongoCursor<RawBsonDocument> cursor = collection.find(query).sort(sort).iterator();
    		
    		BsonValue lastId = null;
    		
    		List<RawBsonDocument> dupesBuffer = new ArrayList<>(2);
    		
    		while (cursor.hasNext()) {
                RawBsonDocument fullDoc = cursor.next();
                BsonValue id = null;
                try {
                    id = fullDoc.get("_id");
                } catch (Exception e) {
                    logger.warn(String.format("%s - Error reading doc id, fullDoc: %s, error: %s",
                            collection.getNamespace(), fullDoc, e));
                    continue;
                }
                
                if (id.equals(lastId)) {
                	dupesBuffer.add(fullDoc);
                	logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                } else {
                	flushDupes(dupesBuffer);
                	dupesBuffer.add(fullDoc);
                }
                lastId = id;
    		}
    		flushAll();
			
		} catch (Exception e) {
			logger.error("call error", e);
			flushAll();
		}
		return null;
	}
	
	
    
    private void flushDupes(List<RawBsonDocument> dupesBuffer) {
		int i = 1;
		for (RawBsonDocument dupe : dupesBuffer) {
			String collName = String.format("%s_%s", base, i++);
			insert(dupe, collName);
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
			//List<BulkWriteError> errors = err.getWriteErrors();
			bulkWriteResult = err.getWriteResult();
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
