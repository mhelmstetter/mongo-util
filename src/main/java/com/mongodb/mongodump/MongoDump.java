package com.mongodb.mongodump;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDump {
    
    private MongoClient mongoClient;
    
    public MongoDump(String mongoUri) {
    	
    	ConnectionString connectionString = new ConnectionString(mongoUri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        mongoClient.getDatabase("admin").runCommand(new Document("ping",1));
    }
    
    private void dump(String databaseName, String collectionName) throws IOException {
        
        File outputFile = new File(databaseName + "." + collectionName + ".bson");
        FileOutputStream fos = null;
        MongoCursor<RawBsonDocument> cursor = null;
        long start = System.currentTimeMillis();
        long count = 0;
        try {
            fos = new FileOutputStream(outputFile);
            FileChannel channel = fos.getChannel();
            MongoDatabase db = mongoClient.getDatabase(databaseName);
            MongoCollection<RawBsonDocument> mongoCollection = db.getCollection(collectionName, RawBsonDocument.class);
            long totalDocs = mongoCollection.countDocuments();
            cursor = mongoCollection.find().iterator();
            
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                ByteBuffer buffer = doc.getByteBuffer().asNIO();
                channel.write(buffer);
                count++;
                if (count % 100000 == 0) {
                    double complete = count/totalDocs*100.0;
                    //System.out.println(count + "/" + totalDocs + "(" + complete + ")");
                    System.out.println(String.format("%s / %s  (%f)", count, totalDocs, count/totalDocs*100.0));
                }
            }
        } finally {
            cursor.close();
            fos.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        System.out.println(String.format("\nDone dumping %s.%s, %s documents in %f seconds", databaseName, collectionName, count, dur));
    }

    public static void main(String args[]) throws Exception {
        if (args.length < 3) {
            throw new IllegalArgumentException("Expected <database> <collection> <mongoUri> arguments");
        }
        String databaseName = args[0];
        String collectionName = args[1];
        String mongoUri = args[2];
        MongoDump mongoDump = new MongoDump(mongoUri);
        mongoDump.dump(databaseName, collectionName);
    }

}
