package com.mongodb.util;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class ConnStormRunnable extends Thread {

    private MongoClient mongoClient;
    private ConnStorm storm;

    private ConnectionString uri;

    public ConnStormRunnable(ConnectionString uri, ConnStorm storm) {
        this.uri = uri;
        this.storm = storm;
    }

    @Override
    public void run() {
        while (true) {
            synchronized (storm) {
                try {
                    storm.wait();
                } catch (InterruptedException e) {
                    System.out.println("interrupted");
                }
            }

            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(uri)
                    .build();
    		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
            mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            mongoClient.close();
        }

    }

    // public void storm() {
    // storm = true;
    // }

}
