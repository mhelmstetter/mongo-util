package com.mongodb.shardsync;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;

public class CleanupOrphaned {
    
    private static Logger logger = LoggerFactory.getLogger(CleanupOrphaned.class);
    
    private List<MongoClient> clients;
    private Map<String, ShardCollection> collectionsMap;
    private Map<String, Shard> shardsMap;
    
    public CleanupOrphaned(List<MongoClient> clients, Map<String, Shard> shardsMap, Map<String, ShardCollection> collectionsMap) {
        this.clients = clients;
        this.shardsMap = shardsMap;
        this.collectionsMap = collectionsMap;
    }
    
    public void cleanupOrphans() {
        ExecutorService executor = Executors.newFixedThreadPool(shardsMap.size());
        for (MongoClient client : clients) {
            Runnable worker = new CleanupOrphanedWorker(client);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }
    
    class CleanupOrphanedWorker implements Runnable {
        
        //private Shard shard;
        private MongoClient client;
        
        public CleanupOrphanedWorker(MongoClient client) {
            this.client = client;
        }
        
        public void run() {
            MongoDatabase db = client.getDatabase("admin");
            for (ShardCollection coll : collectionsMap.values()) {
                
                logger.debug("cleanupOrphaned: " + coll.getNamespace() + " on " + client.getConnectPoint());
                
                Document command = new Document("cleanupOrphaned", coll.getNamespace().toString());
                
                Document result = null;
                Document nextKey = null;
                
                do {
                    if (nextKey != null) {
                        command.append("startingFromKey", nextKey);
                    }
                    result = db.runCommand(command);
                    Double ok = result.getDouble("ok");
                    if (! ok.equals(1.0)) {
                        logger.warn("Cleanup failed: " + result);
                    }
                    nextKey = (Document)result.get("stoppedAtKey");
                    
                } while(nextKey != null);
                
            }
        }
    }

}
