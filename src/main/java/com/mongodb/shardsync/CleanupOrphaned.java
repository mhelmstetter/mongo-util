package com.mongodb.shardsync;

import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

public class CleanupOrphaned {
    
    private static Logger logger = LoggerFactory.getLogger(CleanupOrphaned.class);
    
    private ShardClient shardClient;
    
    public CleanupOrphaned(ShardClient shardClient) {
        this.shardClient = shardClient;
    }
    
    public void cleanupOrphans(Long cleanupOrphansSleepMillis) {
        for (Map.Entry<String, MongoClient> entry : shardClient.getShardMongoClients().entrySet()) {
            MongoClient client = entry.getValue();
            cleanupOrphans(client, entry.getKey(), cleanupOrphansSleepMillis);
        }
        logger.debug("cleanupOrphans complete");
    }
    
    private void cleanupOrphans(MongoClient client, String name, Long cleanupOrphansSleepMillis) {
        try {
            MongoDatabase db = client.getDatabase("admin");
            for (Document coll : shardClient.getCollectionsMap().values()) {
                
                if (coll.get("_id").equals("config.system.sessions")) {
                    continue;
                }
                
                Document command = new Document("cleanupOrphaned", (String)coll.get("_id"));
                command.append("secondaryThrottle", true);
                command.append("writeConcern", new Document("w", "majority"));
                
                Document result = null;
                Document nextKey = null;
                
                do {
                    logger.debug(String.format("cleanupOrphans: %s - %s - nextKey %s", name, coll.get("_id"), nextKey));
                    if (nextKey != null) {
                        command.append("startingFromKey", nextKey);
                    }
                    result = db.runCommand(command);
                    Double ok = result.getDouble("ok");
                    if (! ok.equals(1.0)) {
                        logger.warn("Cleanup failed: " + result);
                    }
                    nextKey = (Document)result.get("stoppedAtKey");
                    
                    if (cleanupOrphansSleepMillis != null) {
                        try {
                            Thread.sleep(cleanupOrphansSleepMillis);
                        } catch (InterruptedException e) {
                        }
                    }
                    
                } while(nextKey != null);
                
            }
        } catch (MongoException me) {
            logger.error("cleanup orphaned error", me);
        }
    }

}
