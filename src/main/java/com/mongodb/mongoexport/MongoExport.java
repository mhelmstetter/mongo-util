package com.mongodb.mongoexport;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "mongoexport", mixinStandardHelpOptions = true, version = "mongoexport 0.1",
description = "Java version of the MongoDB mongoexport command")
public class MongoExport implements Callable<Integer> {
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;
	
	@Option(names = {"-c", "--collection"}, description = "collection to use", required = true)
    private String collectionName;
	
	@Override
    public Integer call() throws Exception { 
        
		ConnectionString connectionString = new ConnectionString(uri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
		
		String dbName = connectionString.getDatabase();
		MongoDatabase db = mongoClient.getDatabase(dbName);
		MongoCursor<Document> cursor = null;
        long start = System.currentTimeMillis();
        long count = 0;
        try {
            
            MongoCollection<Document> mongoCollection = db.getCollection(collectionName);
            long totalDocs = mongoCollection.countDocuments();
            cursor = mongoCollection.find().iterator();
            
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                System.out.println(doc);
                count++;
                if (count % 100000 == 0) {
                    double complete = count/totalDocs*100.0;
                    //System.out.println(count + "/" + totalDocs + "(" + complete + ")");
                    System.err.println(String.format("%s / %s  (%f)", count, totalDocs, count/totalDocs*100.0));
                }
            }
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        System.out.println(String.format("\nDone dumping %s.%s, %s documents in %f seconds", dbName, collectionName, count, dur));
		
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new MongoExport()).execute(args);
        System.exit(exitCode);
    }

}
