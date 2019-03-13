package com.mongodb.util;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class ConnStormRunnable extends Thread {

    private MongoClient mongoClient;
    private ConnStorm storm;

    private MongoClientURI uri;

    public ConnStormRunnable(MongoClientURI uri, ConnStorm storm) {
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

            mongoClient = new MongoClient(uri);
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
