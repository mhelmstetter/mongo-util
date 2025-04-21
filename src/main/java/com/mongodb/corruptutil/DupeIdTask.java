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

public class DupeIdTask implements Callable<DupeIdTaskResult> {

	private static Logger logger = LoggerFactory.getLogger(DupeIdTask.class);
	private final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);

	private final static Bson sort = eq("_id", 1);
	private final static Bson proj = include("_id");

	private MongoCollection<RawBsonDocument> collection;
	private MongoDatabase archiveDb;
	private String base;
	private List<BsonValue> dupesBatch = new ArrayList<>();
	private Map<String, List<WriteModel<RawBsonDocument>>> writeModelsMap = new HashMap<>();

	BsonValue startId;
	BsonValue endId;

	long dupeCount = 0;
	long duplicateDocsCount = 0;
	
	private final static int BATCH_SIZE = 100;

	public DupeIdTask(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, BsonValue startId,
			BsonValue endId) {
		this.collection = collection;
		this.archiveDb = archiveDb;
		this.base = collection.getNamespace().getFullName();
		this.startId = startId;
		this.endId = endId;
	}

	@Override
	public DupeIdTaskResult call() throws Exception {

	    MongoCursor<RawBsonDocument> cursor = null;
	    long count = 0;

	    BsonValue lastId = null;
	    List<BsonValue> duplicateIds = new ArrayList<>();

	    try {
	        Bson query = null;

	        if (startId == null) {
	            query = lt("_id", endId);
	        } else if (endId == null) {
	            query = gte("_id", startId);
	        } else {
	            query = and(gte("_id", startId), lt("_id", endId));
	        }

	        cursor = collection.find(query).projection(proj).sort(sort).iterator();

	        while (cursor.hasNext()) {
	            count++;
	            RawBsonDocument doc = cursor.next();
	            BsonValue id = doc.get("_id");

	            if (id.equals(lastId)) {
	                duplicateIds.add(doc.get("_id"));
	            } else {
	                if (!duplicateIds.isEmpty()) {
	                    duplicateIds.add(lastId);
	                    dupesBatch.addAll(duplicateIds);
	                    processDupesBatch();
	                    duplicateIds.clear();
	                }
	            }

	            lastId = id;

	            if (dupesBatch.size() >= BATCH_SIZE) {
	                processDupesBatch();
	            }
	        }

	        // Process any remaining duplicates
	        if (!duplicateIds.isEmpty()) {
	            duplicateIds.add(lastId);
	            dupesBatch.addAll(duplicateIds);
	            processDupesBatch();
	        }

	    } catch (Exception e) {
	        logger.error("call error", e);
	    } finally {
	        if (cursor != null) {
	            cursor.close();
	        }
	        if (dupesBatch.size() > 0) {
	            processDupesBatch();
	        }
	    }

	    return new DupeIdTaskResult(count, dupeCount, duplicateDocsCount);
	}

	private void processDupesBatch() {
	    if (dupesBatch.isEmpty()) {
	        return;
	    }

	    Bson query = in("_id", dupesBatch);
	    MongoCursor<RawBsonDocument> cursor = collection.find(query).sort(sort).iterator();
	    
	    BsonValue lastId = null;
	    List<RawBsonDocument> duplicateDocs = new ArrayList<>();
	    
	    while (cursor.hasNext()) {
	        RawBsonDocument fullDoc = cursor.next();
	        BsonValue id = fullDoc.get("_id");
	        
	        //logger.debug("startId: {}, current _id: {}", startId, id);
	        
	        if (id.equals(lastId)) {
	            duplicateDocs.add(fullDoc);
	        } else {
	            if (duplicateDocs.size() > 1) {
	                insertDuplicates(duplicateDocs);
	            }
	            duplicateDocs.clear();
	            duplicateDocs.add(fullDoc);
	        }
	        
	        lastId = id;
	    }
	    
	    if (duplicateDocs.size() > 1) {
	        insertDuplicates(duplicateDocs);
	    }
	    
	    cursor.close();
	    flushAll();
	    dupesBatch.clear();
	}

	private void insertDuplicates(List<RawBsonDocument> duplicateDocs) {
		duplicateDocsCount += duplicateDocs.size();
		dupeCount++;
	    int d = 1;
	    for (RawBsonDocument doc : duplicateDocs) {
	        String collName = String.format("%s_%s", base, d++);
	        insert(doc, collName);
	    }
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
		//logger.debug("flushInternal writeModels size: {}", writeModels.size());
		if (writeModels.size() == 0) {
			return;
		}
		BulkWriteResult bulkWriteResult = null;
		try {
			MongoCollection<RawBsonDocument> c1 = archiveDb.getCollection(collName, RawBsonDocument.class);
			bulkWriteResult = c1.bulkWrite(writeModels, bulkWriteOptions);
		} catch (MongoBulkWriteException err) {
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
