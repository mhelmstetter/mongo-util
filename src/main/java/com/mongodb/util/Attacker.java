package com.mongodb.util;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.Document;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class Attacker {
    
    
 
    public static void main(String... args) throws Exception {
        ConnectionString uri = new ConnectionString(args[0]);
        for (int i = 0; i < 64; i++) {
            DdosThread thread = new DdosThread(uri);
            thread.start();
        } 
    } 
 
    public static class DdosThread extends Thread {
 
        private AtomicBoolean running = new AtomicBoolean(true);
        
        private ConnectionString uri;
 
 
        public DdosThread(ConnectionString uri) throws Exception {
            this.uri = uri;
        } 
 
 
        @Override 
        public void run() { 
            //while (running.get()) {
                try { 
                    attack(); 
                } catch (Exception e) {
 
                } 
 
 
            //} 
        } 
 
        public void attack() throws Exception {
        	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(uri)
                    .build();
    		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
            mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
        } 
    } 
 
} 