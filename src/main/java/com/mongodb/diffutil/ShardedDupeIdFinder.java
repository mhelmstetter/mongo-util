package com.mongodb.diffutil;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class ShardedDupeIdFinder {

	private static Logger logger = LoggerFactory.getLogger(ShardedDupeIdFinder.class);

	private static Options options;
	private static CommandLine line;

	private String sourceClusterUri;
	private ShardClient sourceShardClient;
	private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();

	long totalDbs = 0;
	long missingDbs = 0;
	long totalCollections = 0;
	long totalMatches = 0;
	long totalMissingDocs = 0;
	long totalKeysMisordered = 0;
	long totalHashMismatched = 0;
	
	private Set<BsonValue> ids;
	
    private Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private Set<String> collectionsBlacklist = new HashSet<>(Arrays.asList("system.indexes", "system.profile"));
    
    private ExecutorService executor;

	@SuppressWarnings("unchecked")
	public ShardedDupeIdFinder(String sourceClusterUri) {
		this.sourceClusterUri = sourceClusterUri;
		sourceShardClient = new ShardClient("source", sourceClusterUri);
		sourceShardClient.populateShardMongoClients();

		

		Document listDatabases = new Document("listDatabases", 1);
		Document sourceDatabases = sourceShardClient.adminCommand(listDatabases);

		List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
		populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
	}

	private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
		for (Document dbInfo : dbInfoList) {
			databaseMap.put(dbInfo.getString("name"), dbInfo);
		}
	}

	
	public void run() throws InterruptedException {
        executor = Executors.newFixedThreadPool(sourceShardClient.getShardMongoClients().size());
        
		
		for (Map.Entry<String, MongoClient> entry : sourceShardClient.getShardMongoClients().entrySet()) {
            MongoClient sourceClient = entry.getValue();
            String shardName = entry.getKey();
        	
        	for (String dbName : sourceDbInfoMap.keySet()) {
                if (! databasesBlacklist.contains(dbName)) {
                    MongoDatabase db = sourceClient.getDatabase(dbName);
                    logger.debug("db " + dbName);
                    MongoIterable<String> collectionNames = db.listCollectionNames();
                    for (String collectionName : collectionNames) {
                        if (collectionsBlacklist.contains(collectionName)) {
                            continue;
                        }
                        Set<BsonValue> syncSet = Collections.synchronizedSet(new HashSet<>());
        				MongoCollection<RawBsonDocument> sourceColl = db.getCollection(collectionName,
        						RawBsonDocument.class);
                        Runnable worker = new ShardedDupeIdFinderWorker(sourceClient, sourceColl, syncSet, shardName);
                        executor.execute(worker);
                    }
                }
        	}
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(10000);
        }
        logger.debug("ShardedDupeIdFinder complete");
    }

	@SuppressWarnings("static-access")
	private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
		options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(
				OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source").create("s"));

		CommandLineParser parser = new GnuParser();

		try {
			line = parser.parse(options, args);
			if (line.hasOption("help")) {
				printHelpAndExit(options);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println(e.getMessage());
			printHelpAndExit(options);
		} catch (Exception e) {
			e.printStackTrace();
			printHelpAndExit(options);
		}

		return line;
	}

	private static void printHelpAndExit(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("logParser", options);
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = initializeAndParseCommandLineOptions(args);
		ShardedDupeIdFinder finder = new ShardedDupeIdFinder(line.getOptionValue("s"));
		finder.run();
	}

}
