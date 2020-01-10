package com.mongodb.mongostat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.shardsync.ShardClient;

public class MongoStat {
    
    private static Logger logger = LoggerFactory.getLogger(MongoStat.class);
    
    private List<MongoClient> mongoClients = new ArrayList<MongoClient>();
    
    private List<ServerStatus> serverStatuses = new ArrayList<ServerStatus>();
    
    private String[] uris;

    public void setUris(String[] uris) {
        this.uris = uris;
    }

    public void init() {
        for (String uri : uris) {
            logger.debug("Connecting to " + uri);
            ConnectionString connectionString = new ConnectionString(uri);
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .build();
             
            MongoClient client = MongoClients.create(mongoClientSettings);
            
            Document isMaster = client.getDatabase("admin").runCommand(new Document("isMaster", 1));
            logger.debug("isMaster: " + isMaster);
            
            boolean isMongos = false;
            try {
                Document isDbGridResponse = client.getDatabase("admin").runCommand(new Document("isdbgrid", 1));
                Object isDbGrid = isDbGridResponse.get("isdbgrid");
                if (isDbGrid != null) {
                    isMongos = true;
                }
            } catch (MongoCommandException mce) {
            }
            
            if (isMongos) {
                ShardClient sourceShardClient = new ShardClient("source", uri);
                sourceShardClient.populateShardMongoClients();
                Collection<MongoClient> clients = sourceShardClient.getShardMongoClients().values();
                for (MongoClient c : clients) {
                    mongoClients.add(c);
                }
            } else {
                mongoClients.add(client);
            }
            
            ServerStatus status = new ServerStatus();
            serverStatuses.add(status);
        }
        
        
    }

    public void run() {
        while (true) {
            int index = 0;
            System.out.printf(
                    "%-15s%8s%8s%8s%8s %13s%13s%13s%13s %n",
                    "replicaSet", "insert", "query", "update", "delete", "totInserts", "totQueries", "totUpdates", "totDeletes");
            
            for (MongoClient client : mongoClients) {
                Document serverStatus = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));
                
                ServerStatus status = serverStatuses.get(index);
                status.updateServerStatus(serverStatus);
                status.report();
                index++;
            }
            System.out.println();
            sleep(1500);
        }
        
    }
    
    private void sleep(long sleep) {
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
        }
    }

}
