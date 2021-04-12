package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.eq;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class CollectionCloneWorker extends AbstractCollectionCloneWorker implements Runnable {
    
    
    public CollectionCloneWorker(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient, MongoSyncOptions options) {
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
            cursor = sourceCollection.find().sort(eq("$natural", 1)).noCursorTimeout(true).iterator();
            
            Number total = getCount();
            
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                BsonValue id = getId(doc);
                // don't even bother inserting if we couldn't get the _id from the doc
                if (id != null) {
                    writesBuffer.add(new InsertOneModel<RawBsonDocument>(doc));
                    docsBuffer.add(doc);
                } else {
                    logger.warn(String.format("%s - skipping insert, no _id could be read, lastId: %s", ns, lastId));
                    errorCount++;
                }
                
                if (docsBuffer.size() >= options.getBatchSize()) {
                    doInsert();
                    
                    writesBuffer.clear();
                    docsBuffer.clear();
                    
                    long current = System.currentTimeMillis();
                    long delta = (current - last) / 1000;
                    if (delta >= 30) {
                        logger.debug(String.format("%s - cloned %s / %s documents, errorCount: %s, duplicateKeyCount: %s", 
                        		ns, successCount, total, errorCount, duplicateKeyCount));
                        last = current;
                    }
                }
                
                lastId = id;
                
            }
            // flush any remaining from the buffer
            if (docsBuffer.size() > 0) {
                doInsert();
                writesBuffer.clear();
                docsBuffer.clear();
            }
        
        } catch (MongoException me) {
        	logger.error("fatal error cloning collection, ns: {}", ns, me);;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
            
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        logger.debug(String.format("%s - cloned %s documents, errorCount: %s, duplicateKeyCount: %s", ns, successCount, errorCount, duplicateKeyCount));
        logger.debug(String.format("Done cloning %s, %s documents in %f seconds", ns, successCount, dur));
    }

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}
    


}
