package com.mongodb.corruptutil;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class TransactionWorker implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(TransactionWorker.class);

    private MongoClient client;
    
    AtomicInteger atomicInteger = new AtomicInteger();
    private int id;
    private boolean useTransaction;
    private String dbName;

    public TransactionWorker(MongoClient client, int id, boolean useTransaction, String dbName) {
        this.client = client;
        this.id = id;
        this.useTransaction = useTransaction;
        this.dbName = dbName;
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
        	
                
            MongoDatabase db = client.getDatabase(dbName);
            //logger.debug("db " + dbName);
            List<String> collectionNames = new ArrayList<>();
            db.listCollectionNames().into(collectionNames);
            
            logger.debug("TransactionWorker id: {}, collCount: {}", id, collectionNames.size());
            
            for (String collectionName : collectionNames) {
                MongoCollection<RawBsonDocument> coll = db.getCollection(collectionName, RawBsonDocument.class);
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
