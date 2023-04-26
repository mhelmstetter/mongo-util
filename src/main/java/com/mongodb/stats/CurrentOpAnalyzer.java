package com.mongodb.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "currentOpAnalyzer", mixinStandardHelpOptions = true, version = "schemaAnalyzer 0.1")
public class CurrentOpAnalyzer implements Callable<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(CurrentOpAnalyzer.class);
	
	private final static Set<String> ignoreOps = new HashSet<>(Arrays.asList("hello", "isMaster", "ismaster"));
	
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;
	
	@Option(names = "--i", description = "include idle operations")
    boolean idle;
	
	@Option(names = "--d", description = "discover toplogy hosts")
    boolean discover;
	
	
	List<Document> pipeline = new ArrayList<>(1);
	
	
	
	public CurrentOpAnalyzer() {
		Document options = new Document("allUsers", true);
		options.append("idleConnections", true);
		options.append("idleCursors", true);
		options.append("idleSessions", true);
		options.append("localOps", true);
		//Document currentOpPipeline = new Document("$currentOp", options);
		Document currentOpPipeline = new Document("$currentOp", new Document());
		pipeline.add(currentOpPipeline);
	}
	
	private ShardClient shardClient;
	
	private void connect() {
		shardClient = new ShardClient("source", uri);
		shardClient.init();
		shardClient.populateShardMongoClients();
	}
	
	private String getStringValue(RawBsonDocument result, String key) {
		if (result != null && result.containsKey(key)) {
			BsonString bs = result.getString(key);
			if (bs != null) {
				return bs.getValue();
			}
		}
		return null;
	}
	
	private void analyze(MongoClient mongoClient) {
		MongoDatabase db = mongoClient.getDatabase("admin");
		AggregateIterable<RawBsonDocument> it = null;
		int skipCount = 0;
		
		it = db.aggregate(pipeline, RawBsonDocument.class);
		for (RawBsonDocument result : it) {
			String desc = getStringValue(result, "desc");
			
			if (desc == null || !desc.startsWith("conn")) {
				continue;
			}
			
			RawBsonDocument clientMeta = (RawBsonDocument)result.get("clientMetadata");
			if (clientMeta != null) {
				RawBsonDocument driver = (RawBsonDocument)clientMeta.get("driver");
				String driverName = getStringValue(driver, "name");
				if (driverName != null && (driverName.startsWith("NetworkInterface") || driverName.startsWith("MongoDB Internal"))) {
					continue;
				}
				
			}
			
			String appName = getStringValue(result, "appName");
			if (appName != null && (appName.startsWith("MongoDB Automation Agent")
					|| appName.startsWith("MongoDB Monitoring Module")
					|| appName.equals("mongomirror"))) {
				continue;
			}
			
			System.out.print(".");
			
			String client = getStringValue(result, "client");
			
			String op = getStringValue(result, "op");
			
			
			RawBsonDocument cmd = (RawBsonDocument)result.get("command");
			String cmdStr = null;
			String cmdFull = null;
			boolean currentOp = false;
			if (cmd != null && !cmd.isEmpty()) {
				cmdStr = cmd.getFirstKey();
				
				if (ignoreOps.contains(cmdStr)) {
					continue;
				}
				
				cmdFull = cmd.toString();
				if (cmdFull.contains("$currentOp")) {
					continue;
				}
				
				System.out.println(cmdStr);
				
			}
			
			System.out.println("#");
			
			Long secs = null;
			if (result.containsKey("secs_running")) {
				BsonInt64 num = result.getInt64("secs_running");
				if (num != null) {
					secs = num.longValue();
				}
			}
			
			
			
			
			
			
			if (appName != null) {
				System.out.println(appName);
			}
			
			System.out.println(result);
		
			
		}
	}
	
	
	private void analyze() throws IOException {
		
	
		if (shardClient.isMongos()) {
			
			Collection<MongoClient> mongoClients = shardClient.getShardMongoClients().values();
			while (true) {
				for (MongoClient mc : mongoClients) {
					analyze(mc);
				}
			}
			
			
		} else {
			MongoClient mc = shardClient.getMongoClient();
			while (true) {
				analyze(mc);
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
