package com.mongodb.util;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class Attacker {
    
    
 
    public static void main(String... args) throws Exception {
        MongoClientURI uri = new MongoClientURI(args[0]);
        for (int i = 0; i < 64; i++) {
            DdosThread thread = new DdosThread(uri);
            thread.start();
        } 
    } 
 
    public static class DdosThread extends Thread {
 
        private AtomicBoolean running = new AtomicBoolean(true);
        
        private MongoClientURI uri;
 
 
        public DdosThread(MongoClientURI uri) throws Exception {
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
            MongoClient mongoClient = new MongoClient(uri);
            mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
        } 
    } 
 
} 