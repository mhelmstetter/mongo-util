package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.BsonSerializationException;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class ChunkCloneTask implements Callable<ChunkCloneResult> {
	
	
	protected static final Logger logger = LoggerFactory.getLogger(ChunkCloneTask.class);
	
	protected Namespace ns;
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected MongoSyncOptions options;
    protected MongoDatabase sourceDb;
    protected MongoDatabase destDb;
    protected MongoCollection<RawBsonDocument> sourceCollection;
    protected MongoCollection<RawBsonDocument> destCollection;
    
    protected Bson chunkQuery;
    
    
    private final static InsertManyOptions insertManyOptions = new InsertManyOptions().ordered(false);
	
	public ChunkCloneTask(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient, Bson chunkQuery, MongoSyncOptions options) {
        this.ns = ns;
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.options = options;
        this.chunkQuery = chunkQuery;
        
        sourceDb = sourceShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
        sourceCollection = sourceDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
        
        destDb = destShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
        destCollection = destDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
    }

	@Override
	public ChunkCloneResult call() throws Exception {
		
		ChunkCloneResult result = cloneChunk();
		if (result == null) {
			logger.warn("problem cloning chunk, retrying, ns: {}, query: {}", ns, chunkQuery);
			result = cloneChunk();
			if (result != null) {
				logger.debug("chunk clone retry success, ns: {}, query: {}", ns, chunkQuery);
			}
		}
		return result;
	}
	
	private ChunkCloneResult cloneChunk() {
		ChunkCloneResult result = new ChunkCloneResult(ns);
		MongoCursor<RawBsonDocument> cursor = null;
		try {
			
	        List<RawBsonDocument> docsBuffer = new ArrayList<>(options.getBatchSize());
	        
			if (chunkQuery == null) {
	    		cursor = sourceCollection.find().sort(eq("$natural", 1)).noCursorTimeout(true).iterator();
	    	} else {
	    		cursor = sourceCollection.find(chunkQuery).noCursorTimeout(true).iterator();
	    	}
	    	int count = 0;
	        while (cursor.hasNext()) {
	            RawBsonDocument doc = cursor.next();
	            result.sourceCount++;
	            docsBuffer.add(doc);
	            
	            if (docsBuffer.size() >= options.getBatchSize()) {
	                doInsert(docsBuffer, result);
	                docsBuffer.clear();
	            }
	            
	            count++;
	            if (count > 1 && count % 1000000 == 0) {
	            	logger.debug("{}: read {} docs for chunk clone", ns, count);
	            }
	        }
	        // flush any remaining from the buffer
	        if (docsBuffer.size() > 0) {
	            doInsert(docsBuffer, result);
	            docsBuffer.clear();
	        }
		} catch (MongoException me) {
        	logger.error("fatal error cloning chunk, ns: {}", ns, me);
        	result = null;
        } finally {
			try {
				if (cursor != null) {
	        		cursor.close();
	        	}
			} catch (Exception e) {
			}
			
		}
		return result;
		
	}
	
    protected void doInsert(final List<RawBsonDocument> docsBuffer, final ChunkCloneResult result) {
        boolean retry = false;
        
        try {
            destCollection.insertMany(docsBuffer, insertManyOptions);
            result.successCount += docsBuffer.size();
            
        } catch (MongoBulkWriteException bwe) {
            //logger.warn(String.format("%s - insertMany() error : %s", ns, bwe.getMessage()));
        	List<BulkWriteError> errors = bwe.getWriteErrors();
        	
        	int batchDuplicateKeyCount = getDuplicateKeyErrorCount(errors);
        	int batchErrorCount = bwe.getWriteErrors().size() - batchDuplicateKeyCount;
            result.errorCount += batchErrorCount;
            result.duplicateKeyCount += batchDuplicateKeyCount;
            BulkWriteResult bulkWriteResult = bwe.getWriteResult();
            result.successCount += bulkWriteResult.getInsertedCount();
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
                    result.successCount++;
                    prevId = id;
                } catch (MongoException me) {
                    logger.warn(String.format("%s - {_id: %s, prevId: %s, pos: %s} retry using insertOne() unexpected error: %s", ns, id, prevId, pos, me.getMessage()));
                    result.errorCount++;
                }
                pos++;
            }
        }
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

}
