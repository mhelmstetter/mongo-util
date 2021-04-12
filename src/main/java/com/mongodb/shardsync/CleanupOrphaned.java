package com.mongodb.shardsync;

import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Namespace;

public class CleanupOrphaned {
    
    private static Logger logger = LoggerFactory.getLogger(CleanupOrphaned.class);
    
    private ShardClient shardClient;
    private Set<Namespace> includeNamespaces;
    
    public CleanupOrphaned(ShardClient shardClient, Set<Namespace> includeNamespaces) {
        this.shardClient = shardClient;
        this.includeNamespaces = includeNamespaces;
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
                String nsStr = (String)coll.get("_id");
                Namespace ns = new Namespace(nsStr);
                
                if (!includeNamespaces.isEmpty() && !includeNamespaces.contains(ns)) {
                	continue;
                }
                
                Document command = new Document("cleanupOrphaned", ns.getNamespace());
                //command.append("secondaryThrottle", true);
                //command.append("writeConcern", new Document("w", "majority"));
                
                Document result = null;
                Document nextKey = null;
                
                logger.debug(String.format("cleanupOrphans: %s - %s", name, coll.get("_id")));
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
