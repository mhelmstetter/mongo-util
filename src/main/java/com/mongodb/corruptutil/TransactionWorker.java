package com.mongodb.corruptutil;

import java.util.concurrent.atomic.AtomicInteger;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class TransactionWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(TransactionWorker.class);

    private MongoClient client;
    
    AtomicInteger atomicInteger = new AtomicInteger();
    private int id;
    private boolean useTransaction;

    public TransactionWorker(MongoClient client, int id, boolean useTransaction) {
        this.client = client;
        this.id = id;
        this.useTransaction = useTransaction;
    }

    @Override
    public void run() {
    	
    	int count = atomicInteger.incrementAndGet();
    	logger.debug("TransactionWorker start iteration: {}", id);
        
    	ClientSession trxSession =  null;
    	if (useTransaction) {
    		trxSession = client.startSession();
            trxSession.startTransaction();
    	}
    	
        
        try {
        	MongoIterable<String> dbNames = client.listDatabaseNames();
            for (String dbName : dbNames) {
                
                MongoDatabase db = client.getDatabase(dbName);
                //logger.debug("db " + dbName);
                MongoIterable<String> collectionNames = db.listCollectionNames();
                for (String collectionName : collectionNames) {
                    MongoCollection<RawBsonDocument> coll = db.getCollection(collectionName, RawBsonDocument.class);
                    Thread.currentThread().sleep(10);
                }
            }
            if (useTransaction) {
            	trxSession.commitTransaction();
            }
            
            
        } catch (Throwable trxException) {

            logger.error("Exception throw while initializing mongo collections", trxException);
            if (useTransaction) {
            	trxSession.abortTransaction();
            }
            

          } finally {
        	  if (useTransaction) {
        		  trxSession.close();
        	  }
          }
        
    	logger.debug("TransactionWorker complete iteration: {}", id);
    }
        


}
