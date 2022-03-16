package com.mongodb.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "currentOpAnalyzer", mixinStandardHelpOptions = true, version = "schemaAnalyzer 0.1")
public class CurrentOpAnalyzer implements Callable<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(CurrentOpAnalyzer.class);
	
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;
	
	
	MongoClient mongoClient;
	
	private void connect() {
		ConnectionString connectionString = new ConnectionString(uri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
		mongoClient = MongoClients.create(mongoClientSettings);
	}
	
	
	
	private void analyze() throws IOException {
		
		MongoDatabase db = mongoClient.getDatabase("admin");
		List<Document> pipeline = new ArrayList<>(1);
		pipeline.add(new Document("$currentOp", new Document()));
		AggregateIterable<Document> it = null; 
		for (int i = 0; i < 1000; i++) {
			it = db.aggregate(pipeline);
			for (Document result : it) {
				Document meta = (Document)result.get("clientMetadata");
				if (meta != null) {
					Document driver = (Document)meta.get("driver");
					System.out.println(driver.hashCode());
				}
				
			}
		}
	
	
		
		
	}
	
	
	@Override
    public Integer call() throws Exception { 
        
		connect();
		analyze();
		
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new CurrentOpAnalyzer()).execute(args);
        System.exit(exitCode);
    }

}
