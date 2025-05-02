package com.mongodb.corruptutil;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

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

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "dupe-util",
    mixinStandardHelpOptions = true,
    version = "DupeUtil 1.0",
    description = "Finds duplicate _id values in MongoDB collections"
)
public class DupeUtil implements Callable<Integer> {
    
    private static Logger logger = LoggerFactory.getLogger(DupeUtil.class);
    
    private Set<String> databasesExcludeList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private Set<String> collectionsExcludeList = new HashSet<>(Arrays.asList("system.indexes", "system.profile"));
    
    private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
    private boolean filtered;
    
    @Option(names = {"-s", "--source"}, required = true, description = "Source cluster connection URI")
    private String sourceUriStr;
    
    @Option(names = {"-d", "--dest"}, description = "Destination (archive) cluster connection URI")
    private String destUriStr;
    
    @Option(names = {"-a", "--archive"}, description = "Archive database name")
    private String archiveDbName;
    
    @Option(names = {"-i", "--startId"}, description = "Starting id ($gte)")
    private String startIdStr;
    
    @Option(names = {"-t", "--threads"}, description = "Number of threads to use (default: 4)")
    private int threads = 4;
    
    @Option(names = {"-f", "--filter"}, description = "Namespace filter (can be specified multiple times)", split = ",")
    private String[] filters;
    
    private MongoClient sourceClient;
    private MongoClient destClient;
    
    private MongoDatabase archiveDb;
    
    private Integer startId;
    
    private ExecutorService executor;
    
    private final Set<String> startingCollectionNames = new HashSet<>();
    private final Set<String> finalCollectionNames = new HashSet<>();
    private final Set<String> newCollectionNames = new HashSet<>();
    
    List<DupeIdCollectionWorker> workers = new ArrayList<>();
    
    // Default constructor for Picocli
    public DupeUtil() {
    }
    
    // Constructor for programmatic use
    public DupeUtil(String sourceUriStr, String destUriStr, String archiveDbName, String startIdStr) {
        this.sourceUriStr = sourceUriStr;
        this.destUriStr = destUriStr;
        this.archiveDbName = archiveDbName;
        this.startIdStr = startIdStr;
        
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
    
    /**
     * Adds namespace filters for processing specific collections.
     * 
     * @param filters Array of namespace strings to include
     */
    public void addFilters(String[] filters) {
        this.filtered = filters != null && filters.length > 0;
        if (!filtered) {
            return;
        }
        
        for (String f : filters) {
            Namespace ns = new Namespace(f);
            includeNamespaces.add(ns);
        }
    }
    
    private void initializeClients() {
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
        
        // Apply filters if specified via command line
        if (filters != null && filters.length > 0) {
            addFilters(filters);
        }
    }
    
    @Override
    public Integer call() throws Exception {
        // Initialize clients if not already done (when run via main)
        if (sourceClient == null) {
            initializeClients();
        }
        
        long dupeCount = run();
        return dupeCount > 0 ? 1 : 0; // Return non-zero exit code if duplicates found
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
        if (archiveDb == null) {
            return;
        }
        
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
    
    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setArchiveDb(MongoDatabase archiveDb) {
        this.archiveDb = archiveDb;
    }
    
    public static void main(String[] args) {
        int exitCode = new CommandLine(new DupeUtil()).execute(args);
        System.exit(exitCode);
    }
}