package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class DupeIdFinderWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DupeIdFinderWorker.class);

    private MongoCollection<RawBsonDocument> collection;
    private MongoClient client;
    
    private MongoDatabase archiveDb;
    
    private final static int BATCH_SIZE = 1000;
    
    private final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions().ordered(false);
    
    private Map<String, List<WriteModel<RawBsonDocument>>> writeModelsMap = new HashMap<>();
    
    private List<BsonValue> dupesBatch = new ArrayList<>(BATCH_SIZE);
    
    private String base;
    
    private Integer startId;
    
    Bson sort = eq("_id", 1);

    public DupeIdFinderWorker(MongoClient client, MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, Integer startId) {
    	collection.getNamespace();
        this.collection = collection;
        this.client = client;
        this.archiveDb = archiveDb;
        this.startId = startId;
        this.base = collection.getNamespace().getFullName();
    }
    

    private void handleDupes(RawBsonDocument doc) {
    	if (archiveDb != null) {
    		
    		dupesBatch.add(doc.get("_id"));
    		if (dupesBatch.size() <= BATCH_SIZE) {
    			
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
    
    private void insert(RawBsonDocument fullDoc, String collName) {
    	WriteModel<RawBsonDocument> model = new InsertOneModel<>(fullDoc);
    	
    	List<WriteModel<RawBsonDocument>> writeModels = writeModelsMap.get(collName);
    	if (writeModels == null) {
    		writeModels = new ArrayList<>();
    		writeModelsMap.put(collName, writeModels);
    	}
    	
		writeModels.add(model);
		if (writeModels.size() >= BATCH_SIZE) {
			flush(collName);
		}
    	
    }
    
    private void flush(String collName) {
    	List<WriteModel<RawBsonDocument>> writeModels = writeModelsMap.get(collName);
    	flushInternal(collName, writeModels);
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

    @Override
    public void run() {
        MongoCursor<RawBsonDocument> cursor = null;
        long start = System.currentTimeMillis();
        long last = start;
        long count = 0;
        long dupeCount = 0;

        try {
        	Bson proj = include("_id");
    		
    		
    		Bson query = null;
    		if (startId != null) {
    			query = gte("_id", startId);
    		}
    		
            cursor = collection.find(query).projection(proj).sort(sort).iterator();
            Number total = collection.estimatedDocumentCount();
            BsonValue lastId = null;
            //RawBsonDocument lastDocument = null;
            
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

                long current = System.currentTimeMillis();
                long delta = (current - last) / 1000;
                if (delta >= 30) {
                    logger.debug(String.format("%s - checked %s / %s documents, dupe id count: %s",
                            collection.getNamespace(), count, total, dupeCount));
                    last = current;
                }
            }

        } finally {
            cursor.close();
            flushAll();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start) / 1000.0;
        logger.debug(String.format("Done dupe _id check %s, %s documents in %f seconds, dupe id count: %s",
                collection.getNamespace(), count, dur, dupeCount));

    }

}
