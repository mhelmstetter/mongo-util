package com.mongodb.corruptutil;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.model.Namespace;

public class DupeUtil {
    
    private static Logger logger = LoggerFactory.getLogger(DupeUtil.class);
    
    private Set<String> databasesExcludeList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private Set<String> collectionsExcludeList = new HashSet<>(Arrays.asList("system.indexes", "system.profile"));
    
    private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
    private boolean filtered;
    
    private static Options options;
    
    private MongoClient sourceClient;
    private MongoClient destClient;
    
    private MongoDatabase archiveDb;
    
    private int threads = 4;
    
    private ExecutorService executor;
    
    private Integer startId;
    
    private final Set<String> startingCollectionNames = new HashSet<>();
    private final Set<String> finalCollectionNames = new HashSet<>();
    private final Set<String> newCollectionNames = new HashSet<>();
    
    List<DupeIdCollectionWorker> workers = new ArrayList<>();
    
    public DupeUtil(String sourceUriStr, String destUriStr, String archiveDbName, String startIdStr) {
    	ConnectionString connectionString = new ConnectionString(sourceUriStr);
    	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
    	sourceClient = MongoClients.create(mongoClientSettings);
    	
    	if (destUriStr != null) {
    		ConnectionString cs = new ConnectionString(destUriStr);
        	MongoClientSettings mcs = MongoClientSettings.builder()
                    .applyConnectionString(cs)
                    .build();
        	destClient = MongoClients.create(mcs);
    	} else {
    		destClient = sourceClient;
    	}
    	
    	if (archiveDbName != null) {
        	archiveDb = destClient.getDatabase(archiveDbName);
        }
    	
    	if (startIdStr != null) {
    		startId = Integer.parseInt(startIdStr);
    	}
    	populateCollectionNames(startingCollectionNames);
    }
    
    public long run() throws InterruptedException {
    	logger.debug("DupeUtil starting");
        executor = Executors.newFixedThreadPool(threads);
        MongoIterable<String> dbNames = sourceClient.listDatabaseNames();
        for (String dbName : dbNames) {
            if (! databasesExcludeList.contains(dbName)) {
                MongoDatabase db = sourceClient.getDatabase(dbName);
                MongoIterable<String> collectionNames = db.listCollectionNames();
                for (String collectionName : collectionNames) {
                    if (collectionsExcludeList.contains(collectionName)) {
                        continue;
                    }
                    Namespace ns = new Namespace(dbName, collectionName);
                    if (filtered && !includeNamespaces.contains(ns)) {
						continue;
					}
                    
                    MongoCollection<RawBsonDocument> coll = db.getCollection(collectionName, RawBsonDocument.class);
                    DupeIdCollectionWorker worker = new DupeIdCollectionWorker(coll, archiveDb, threads);
                    workers.add(worker);
                    executor.execute(worker);
                }
            }
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(1000);
        }
        
        long totalCount = 0;
        long dupeCount = 0;
        for (DupeIdCollectionWorker worker : workers) {
        	totalCount += worker.getCount();
        	dupeCount += worker.getDupeCount();
        }
        
        NumberFormat formatter = NumberFormat.getInstance();
        logger.debug("DupeUtil complete, found {} duplicates out of {} docs", 
                     formatter.format(dupeCount), formatter.format(totalCount));
        
        computeNewCollections();
        logger.debug("new collections: {}", newCollectionNames);
        
        return dupeCount;
    }
    
	/**
	 * Computes the set difference between finalCollectionNames and startingCollectionNames.
	 * The result contains all elements that are in finalCollectionNames but not in startingCollectionNames.
	 */
	public void computeNewCollections() {
	    // Clear any existing entries in newCollections
		newCollectionNames.clear();
	    
	    // Add all elements from finalCollectionNames
		newCollectionNames.addAll(finalCollectionNames);
	    
	    // Remove all elements that are also in startingCollectionNames
		newCollectionNames.removeAll(startingCollectionNames);
	}
	
	public void populateCollectionNames(Set<String> names) {
	    
	    try (MongoCursor<String> cursor = archiveDb.listCollectionNames().iterator()) {
	        while (cursor.hasNext()) {
	            names.add(cursor.next());
	        }
	    }
	}
	
	/**
	 * Iterates through collections ending with "_1" and deletes related documents 
	 * from corresponding collections.
	 * Collection names follow pattern: <dbName>_collectionName_[1-3]
	 *
	 * @param mongoClient The MongoDB client instance
	 * @param newCollectionNames Set of collection names to process
	 * @param batchSize Size of batches for deletion operations
	 */
	public void processAndDeleteRelatedDocuments(MongoClient mongoClient, 
	                                           Set<String> newCollectionNames,
	                                           int batchSize) {
	    // Filter for collections ending with "_1"
	    Set<String> sourceCollections = newCollectionNames.stream()
	            .filter(name -> name.endsWith("_1"))
	            .collect(Collectors.toSet());
	    
	    for (String sourceCollection : sourceCollections) {
	        // Parse dbName and collectionName from the pattern <dbName>_collectionName_1
	        String[] parts = sourceCollection.split("_");
	        if (parts.length < 3) {
	            logger.warn("Invalid collection name format: " + sourceCollection);
	            continue;
	        }
	        
	        String dbName = parts[0];
	        // Reconstruct the collection base name without the suffix "_1"
	        String collectionBaseName = String.join("_", 
	            Arrays.copyOfRange(parts, 1, parts.length - 1));
	        
	        // Get the source collection to query IDs from
	        MongoCollection<Document> collection = mongoClient
	            .getDatabase(dbName)
	            .getCollection(sourceCollection);
	        
	        // Query only the _id fields
	        FindIterable<Document> documents = collection.find()
	            .projection(Projections.include("_id"));
	        
	        List<ObjectId> idBatch = new ArrayList<>(batchSize);
	        
	        // Iterate through the IDs and delete in batches
	        for (Document doc : documents) {
	            idBatch.add(doc.getObjectId("_id"));
	            
	            // When batch is full, delete from target collection and reset batch
	            if (idBatch.size() >= batchSize) {
	                deleteFromTargetCollection(mongoClient, dbName, collectionBaseName, idBatch);
	                idBatch.clear();
	            }
	        }
	        
	        // Process any remaining IDs in the final batch
	        if (!idBatch.isEmpty()) {
	            deleteFromTargetCollection(mongoClient, dbName, collectionBaseName, idBatch);
	        }
	    }
	}

	/**
	 * Deletes documents from the target collection using the provided IDs.
	 *
	 * @param mongoClient MongoDB client
	 * @param dbName Database name
	 * @param collectionBaseName Base collection name
	 * @param idBatch List of ObjectIds to delete
	 */
	private void deleteFromTargetCollection(MongoClient mongoClient, 
	                                     String dbName, 
	                                     String collectionBaseName,
	                                     List<ObjectId> idBatch) {

		MongoCollection<Document> targetCollection = mongoClient
	        .getDatabase(dbName)
	        .getCollection(collectionBaseName);
	    
	    // Create the $in query for deletion
	    Bson deleteFilter = Filters.in("_id", idBatch);
	    
	    // Perform the deletion
	    DeleteResult result = targetCollection.deleteMany(deleteFilter);
	    logger.info("Deleted " + result.getDeletedCount() + " documents from " + 
	                dbName + "." + collectionBaseName);
	}
    
    public void addFilters(String[] filters) {
    	this.filtered = filters != null;
    	if (filters == null) {
    		return;
    	}
    	for (String f : filters) {
    		Namespace ns = new Namespace(f);
    		includeNamespaces.add(ns);
    	}
    }
    
    public void setThreads(int threads) {
        this.threads = threads;
    }

    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(Option.builder("s").desc("Source cluster connection uri").hasArgs().longOpt("source")
                .required(true).build());
        options.addOption(Option.builder("d").desc("Destination (archive) cluster connection uri").hasArgs().longOpt("dest").build());
        options.addOption(Option.builder("t").desc("# threads").hasArgs().longOpt("threads").build());
        options.addOption(Option.builder("f").desc("namespace filter").hasArgs().longOpt("filter").build());
        options.addOption(Option.builder("a").desc("archive database name").hasArgs().longOpt("archive").build());
        options.addOption(Option.builder("i").desc("starting id ($gte)").hasArgs().longOpt("startId").build());
        

        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
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
        DupeUtil util = new DupeUtil(line.getOptionValue("s"), line.getOptionValue("d"), line.getOptionValue("a"), line.getOptionValue("i"));
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            util.setThreads(threads);
        }
        
        String[] filters = line.getOptionValues("f");
        util.addFilters(filters);
        
        util.run();

    }

	public void setArchiveDb(MongoDatabase archiveDb) {
		this.archiveDb = archiveDb;
	}

}
