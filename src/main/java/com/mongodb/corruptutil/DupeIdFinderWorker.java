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

public class DupeIdFinderWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(DupeIdFinderWorker.class);

    private MongoCollection<RawBsonDocument> collection;
    private MongoClient client;

    public DupeIdFinderWorker(MongoClient client, MongoCollection<RawBsonDocument> collection) {
        this.collection = collection;
        this.client = client;
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
    		Bson sort = eq("_id", 1);
    		
            cursor = collection.find().noCursorTimeout(true).projection(proj).sort(sort).iterator();
            Number total = collection.estimatedDocumentCount();
            BsonValue lastId = null;
            while (cursor.hasNext()) {
                count++;
                RawBsonDocument doc = cursor.next();
                BsonValue id = null;
                try {
                    id = doc.get("_id");
                } catch (Exception e) {
                    logger.warn(String.format("%s - Error reading doc id, count: %s, error: %s",
                            collection.getNamespace(), count, e));
                    //handleCorrupt(doc, id);
                    continue;
                }
                
                if (id.equals(lastId)) {
                	dupeCount++;
                	logger.warn("duplicate _id found for _id: {}", id);
                }
                lastId = id;

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
