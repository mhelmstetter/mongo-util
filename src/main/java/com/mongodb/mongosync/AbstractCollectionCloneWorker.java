package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonSerializationException;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public abstract class AbstractCollectionCloneWorker {
    
    protected static final Logger logger = LoggerFactory.getLogger(AbstractCollectionCloneWorker.class);

    public abstract void run();

    protected Namespace ns;
    protected Document shardCollection;
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected MongoSyncOptions options;
    protected MongoDatabase sourceDb;
    protected MongoDatabase destDb;
    protected MongoCollection<RawBsonDocument> sourceCollection;
    protected MongoCollection<RawBsonDocument> destCollection;
    
    protected List<RawBsonDocument> docsBuffer;
    
    List<WriteModel<RawBsonDocument>> writesBuffer;
    
    //protected List<Document> hashesBuffer;
    //protected InsertManyOptions insertManyOptions;
    protected final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
    
    protected long successCount;
    protected long errorCount;
    protected long duplicateKeyCount;
    protected BsonValue lastId;
    protected BsonValue previousBatchLastId;

    public AbstractCollectionCloneWorker(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient, MongoSyncOptions options) {
        this.ns = ns;
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.options = options;
        
        if (ns != null) {
        	sourceDb = sourceShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
            sourceCollection = sourceDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
            
            destDb = destShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
            destCollection = destDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
            shardCollection = sourceShardClient.getCollectionsMap().get(ns.getNamespace());
        }
        
        
        docsBuffer = new ArrayList<RawBsonDocument>(options.getBatchSize());
        writesBuffer = new ArrayList<>(options.getBatchSize());
        
        //hashesBuffer = new ArrayList<Document>(options.getBatchSize());
        
        
        //insertManyOptions = new InsertManyOptions();
        //insertManyOptions.ordered(false);
    }
    
    protected static BsonValue getId(RawBsonDocument doc) {
        BsonValue lastId = null;
        try {
            lastId = doc.get("_id");
        } catch (BsonSerializationException bse) {
            //logger.warn("Failed to get _id, previous _id was: " + lastId);
        }
        return lastId;
    }
    
    private int getDuplicateKeyErrorCount(List<BulkWriteError> errors) {
    	int count = 0;
    	for (BulkWriteError bwe : errors) {
    		if (bwe.getCode() == 11000) {
    			count++;
    		} else {
    			logger.warn(String.format("%s - insertMany() error : %s", ns, bwe.getMessage()));
    		}
    	}
    	return count;
    }
    
    protected Number getCount() {
    	Number total = null;
    	try {
    		total = ShardClient.countDocuments(sourceDb, sourceCollection);
            logger.debug(String.format("%s - count: %s documents", ns, total));
            return total;
            
    	} catch (MongoException me) {
    		logger.warn("error getting collection count for ns: {}. will retry with estimated count, message: {}", ns, me.getMessage());
    	}
    	
    	try {
    		total = ShardClient.estimatedDocumentCount(sourceDb, sourceCollection);
            logger.debug(String.format("%s - count: %s documents", ns, total));
            return total;
    	} catch (MongoException me) {
    		logger.warn("2nd attempt, error getting estimated collection count for ns: {}, message: {}", ns, me.getMessage());
    	}
    	
    	return total;
    }
    
    protected void doInsert() {
        boolean retry = false;
        
        
        try {
            //destCollection.insertMany(buffer, insertManyOptions);
            BulkWriteResult result = destCollection.bulkWrite(writesBuffer, bulkWriteOptions);
            successCount += result.getInsertedCount();
            
            // this would be double counting?
            //errorCount += docsBuffer.size() - result.getInsertedCount();
            
            
        } catch (MongoBulkWriteException bwe) {
            //logger.warn(String.format("%s - insertMany() error : %s", ns, bwe.getMessage()));
        	List<BulkWriteError> errors = bwe.getWriteErrors();
        	int batchDuplicateKeyCount = getDuplicateKeyErrorCount(errors);
        	int batchErrorCount = bwe.getWriteErrors().size() - batchDuplicateKeyCount;
            errorCount += batchErrorCount;
            duplicateKeyCount += batchDuplicateKeyCount;
        } catch (MongoException e) {
            logger.warn(String.format("%s - insertMany() unexpected error: %s", ns, e.getMessage()));
            //errorCount++;
            retry = true;
        }
        
        if (retry) {
            //logger.debug(String.format("%s - Starting retry block, docsBuffer size: %s", ns, docsBuffer.size()));
            
            int pos = 0;
            BsonValue prevId = null;
            BsonValue id = null;
            for (RawBsonDocument doc : docsBuffer) {
                
                try {
                    id = getId(doc);
                    destCollection.insertOne(doc);
                    //logger.debug("inserted " + id);
                    successCount++;
                    prevId = id;
                } catch (MongoException me) {
                    logger.warn(String.format("%s - {_id: %s, prevId: %s, pos: %s} retry using insertOne() unexpected error: %s", ns, id, prevId, pos, me.getMessage()));
                    errorCount++;
                }
                pos++;
            }
        }
    }

}