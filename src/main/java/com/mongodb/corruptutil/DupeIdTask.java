package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    
    private final static int BATCH_SIZE = 1000;

    public DupeIdTask(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, BsonValue startId, BsonValue endId) {
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
        long dupeCount = 0;
        BsonValue lastId = null;
        List<RawBsonDocument> duplicateDocs = new ArrayList<>();
        
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
                BsonValue id = null;
                try {
                    id = doc.get("_id");
                } catch (Exception e) {
                    logger.warn(String.format("%s - Error reading doc id, count: %s, error: %s",
                            collection.getNamespace(), count, e));
                    continue;
                }
                
                if (id.equals(lastId)) {
                    duplicateDocs.add(doc);
                    dupeCount++;
                    logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                } else {
                    if (!duplicateDocs.isEmpty()) {
                        handleDupes(duplicateDocs);
                        duplicateDocs.clear();
                    }
                }
                lastId = id;
            }
            
            // Handle any remaining duplicates
            if (!duplicateDocs.isEmpty()) {
                handleDupes(duplicateDocs);
            }
        } catch (Exception e) {
            logger.error("call error", e);
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            flushAll();
        }
        return new DupeIdTaskResult(count, dupeCount);
    }
    
    private void handleDupes(List<RawBsonDocument> docs) {
        if (archiveDb != null) {
            Set<BsonValue> processedIds = new HashSet<>();
            int d = 1;

            for (RawBsonDocument doc : docs) {
                BsonValue docId = doc.get("_id");

                if (!processedIds.add(docId)) {
                    // This id has already been processed, continue to the next one
                    continue;
                }

                dupesBatch.add(docId);

                if (dupesBatch.size() >= BATCH_SIZE) {
                    Bson query = in("_id", dupesBatch);
                    MongoCursor<RawBsonDocument> cursor = collection.find(query).sort(sort).iterator();
                    
                    try {
                        BsonValue lastId = null;
                        while (cursor.hasNext()) {
                            RawBsonDocument fullDoc = cursor.next();
                            BsonValue id = fullDoc.get("_id");

                            if (id.equals(lastId)) {
                                logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                                String collName = String.format("%s_%s", base, d++);
                                insert(fullDoc, collName);
                            } else {
                                d = 1;
                            }
                            lastId = id;
                        }
                    } catch (Exception e) {
                        logger.error("Error processing duplicates", e);
                    } finally {
                        cursor.close();
                    }
                    dupesBatch.clear();
                }
            }
        }
    }
    
    private void insert(RawBsonDocument doc, String collName) {
        List<WriteModel<RawBsonDocument>> models = writeModelsMap.computeIfAbsent(collName, k -> new ArrayList<>());
        models.add(new InsertOneModel<>(doc));
        if (models.size() >= BATCH_SIZE) {
            flush(collName, models);
        }
    }
    
    private void flush(String collName, List<WriteModel<RawBsonDocument>> models) {
        try {
            MongoCollection<RawBsonDocument> coll = archiveDb.getCollection(collName, RawBsonDocument.class);
            BulkWriteResult result = coll.bulkWrite(models, bulkWriteOptions);
            logger.info("flushed {} records to {}", result.getInsertedCount(), collName);
        } catch (MongoBulkWriteException e) {
            logger.error("Bulk write error", e);
        }
        models.clear();
    }
    
    private void flushAll() {
        writeModelsMap.forEach(this::flush);
    }
}
