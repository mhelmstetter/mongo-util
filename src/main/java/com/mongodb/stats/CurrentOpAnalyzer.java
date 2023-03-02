package com.mongodb.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.Document;
import org.bson.RawBsonDocument;
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
	
	private final static Set<String> ignoreOps = new HashSet<>(Arrays.asList("hello", "isMaster"));
	
	
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
		AggregateIterable<RawBsonDocument> it = null; 
		while (true) {
			it = db.aggregate(pipeline, RawBsonDocument.class);
			for (RawBsonDocument result : it) {
				String desc = result.getString("desc").getValue();
				String op = result.getString("op").getValue();
				RawBsonDocument cmd = (RawBsonDocument)result.get("command");
				String cmdStr = null;
				if (!cmd.isEmpty()) {
					cmdStr = cmd.getFirstKey();
				}
				
				Long secs = null;
				if (result.containsKey("secs_running")) {
					BsonInt64 num = result.getInt64("secs_running");
					if (num != null) {
						secs = num.longValue();
					}
				}
				
				String cmdFull = cmd.toString();
				boolean currentOp = cmdFull.contains("$currentOp");
				
				if (!currentOp && cmdStr != null && !ignoreOps.contains(cmdStr)) {
					System.out.println(desc + " " + op + " " + cmdStr + " " + secs);
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
