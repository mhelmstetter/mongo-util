package com.mongodb.mongosync;

import org.bson.BsonSerializationException;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;

import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class DummyCloneWorker extends AbstractCollectionCloneWorker implements Runnable {
    
    private DocumentCodec codec = new DocumentCodec();
    
    public DummyCloneWorker(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient, MongoSyncOptions options) {
        super(ns, sourceShardClient, destShardClient, options);
    }
    

    @Override
    public void run() {
        
        MongoCursor<RawBsonDocument> cursor = null;
        
        long start = System.currentTimeMillis();
        long last = start;
        successCount = 0;
        errorCount = 0;
        try {
            cursor = sourceCollection.find().noCursorTimeout(true).hint(new Document("_id", 1)).iterator();
            //Number total = ShardClient.getFastCollectionCount(sourceDb, sourceCollection);
            logger.debug(String.format("%s - starting", ns));
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                BsonValue id = getId(doc);
                
                // don't even bother inserting if we couldn't get the _id from the doc
                if (id != null) {
                    try {
                        Document resultDoc = doc.decode(codec);
                    } catch (BsonSerializationException bse) {
                        logger.warn(String.format("%s - skipping corrupt doc, _id: %s", ns, id));
                        errorCount++;
                    }
                    successCount++;
                    

                } else {
                    logger.warn(String.format("%s - skipping corrupt doc, no _id could be read, lastId: %s", ns, lastId));
                    errorCount++;
                }
                
                lastId = id;
                
                if (successCount % 500000 == 0) {
                    long current = System.currentTimeMillis();
                    long delta = (current - last) / 1000;
                    logger.debug(String.format("%s - checked %s documents, errorCount: %s", ns, successCount, errorCount));
                    last = current;
                }
                
                
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        logger.debug(String.format("Done cloning %s, %s documents in %f seconds. errorCount: %s", ns, successCount, dur, errorCount));
    }
    


}
