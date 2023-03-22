package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.include;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.bson.BsonSerializationException;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class DupeIdFinderWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DupeIdFinderWorker.class);

    private MongoCollection<RawBsonDocument> collection;
    private MongoClient client;
    
    private MongoDatabase archiveDb;

    public DupeIdFinderWorker(MongoClient client, MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb) {
    	collection.getNamespace();
        this.collection = collection;
        this.client = client;
        this.archiveDb = archiveDb;
    }
    

    private void handleDupes(RawBsonDocument doc, RawBsonDocument lastDoc, int dupeNum) {
    	if (archiveDb != null) {
    		String base = collection.getNamespace().getFullName();
    		MongoCollection<RawBsonDocument> c1 = archiveDb.getCollection(String.format("%s_%s", base, dupeNum-1), RawBsonDocument.class);
    		MongoCollection<RawBsonDocument> c2 = archiveDb.getCollection(String.format("%s_%s", base, dupeNum), RawBsonDocument.class);
    		c1.insertOne(doc);
    		c2.insertOne(lastDoc);
    	}
    	
    }

    @Override
    public void run() {
        MongoCursor<RawBsonDocument> cursor = null;
        long start = System.currentTimeMillis();
        long last = start;
        long count = 0;
        long dupeCount = 0;
        int dupeNum = 1;

        try {
        	Bson proj = include("_id");
    		Bson sort = eq("_id", 1);
    		
            cursor = collection.find().projection(proj).sort(sort).iterator();
            Number total = collection.estimatedDocumentCount();
            BsonValue lastId = null;
            RawBsonDocument lastDocument = null;
            
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
                	handleDupes(doc, lastDocument, dupeNum);
                	dupeNum++;
                	dupeCount++;
                	logger.warn("{} - duplicate _id found for _id: {}", collection.getNamespace(), id);
                } else {
                	dupeNum = 1;
                }
                lastId = id;
                lastDocument = doc;

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
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start) / 1000.0;
        logger.debug(String.format("Done dupe _id check %s, %s documents in %f seconds, dupe id count: %s",
                collection.getNamespace(), count, dur, dupeCount));

    }

}
