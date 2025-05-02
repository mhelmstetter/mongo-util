package com.mongodb.corruptutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.types.MinKey;
import org.bson.types.MaxKey;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static com.mongodb.client.model.Filters.eq;

@Command(name = "MongoDataLoader", mixinStandardHelpOptions = true, 
         description = "Loads dummy data into MongoDB collections with optional sharding")
public class MongoDataLoader implements Callable<Integer> {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoDataLoader.class);

    @Option(names = {"-u", "--uri"}, description = "MongoDB connection URI", required = true)
    private String uri;

    @Option(names = {"-d", "--database"}, description = "Target database name", defaultValue = "db1")
    private String databaseName;

    @Option(names = {"-c", "--collection"}, description = "Target collection name", defaultValue = "c1")
    private String collectionName;

    @Option(names = {"-n", "--num-docs"}, description = "Number of documents to insert", defaultValue = "10000")
    private int numDocuments;

    @Option(names = {"-b", "--batch-size"}, description = "Batch size for inserts", defaultValue = "1000")
    private int batchSize;

    @Option(names = {"-t", "--threads"}, description = "Number of worker threads", defaultValue = "4")
    private int numThreads;
    
    @Option(names = {"--insert-dupes"}, description = "Insert duplicate _id values (~1% of documents)", defaultValue = "true")
    private boolean insertDupes;
    
    @Option(names = {"--shard-chunks"}, description = "Number of chunks to pre-split", defaultValue = "16")
    private int numShardChunks;
    
    @Option(names = {"--drop"}, description = "Drop the collection if it already exists", defaultValue = "true")
    private boolean dropCollection;

    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Random random = new Random();
    private static final Set<ObjectId> generatedIds = new HashSet<>();
    private static final Set<ObjectId> duplicateIds = new HashSet<>();
    private static final Map<Integer, Integer> shardDistributionMap = new HashMap<>();
    
    private MongoClient mongoClient;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new MongoDataLoader()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        logger.info("Connecting to MongoDB at: {}", uri);
        logger.info("Target namespace: {}.{}", databaseName, collectionName);
        logger.info("Documents to insert: {}", numDocuments);
        logger.info("Using {} threads", numThreads);
        logger.info("Insert duplicates: {}", insertDupes);
        logger.info("Drop existing collection: {}", dropCollection);
        
        // Pre-calculate approximate number of duplicate documents (~1%)
        int duplicateCount = insertDupes ? Math.max(1, numDocuments / 100) : 0;
        if (insertDupes) {
            logger.info("Will insert approximately {} documents with duplicate _ids", duplicateCount);
        }

        // Initialize MongoDB client with modern driver
        ConnectionString connectionString = new ConnectionString(uri);
        MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
            .applyConnectionString(connectionString)
            .build();
        
        try (MongoClient client = MongoClients.create(mongoClientSettings)) {
            // Store reference to the client for use in other methods
            this.mongoClient = client;
            
            MongoDatabase database = client.getDatabase(databaseName);
            
            // Check if the collection exists
            boolean collectionExists = database.listCollectionNames()
                    .into(new ArrayList<>())
                    .contains(collectionName);
                    
            if (collectionExists) {
                if (!dropCollection) {
                    // Collection exists and should not be dropped - throw fatal error
                    String errorMsg = String.format(
                        "Collection %s.%s already exists and --drop flag is not set",
                        databaseName, collectionName
                    );
                    logger.error(errorMsg);
                    return 1; // Return error code
                } else {
                    // Drop the collection as requested
                    logger.info("Dropping existing collection: {}", collectionName);
                    database.getCollection(collectionName).drop();
                    
                    // Create a new collection
                    logger.info("Creating collection: {}", collectionName);
                    database.createCollection(collectionName);
                }
            } else {
                // Collection doesn't exist, create it
                logger.info("Creating collection: {}", collectionName);
                database.createCollection(collectionName);
            }
            
            setupRangeSharding(client, database);
            
            // Set up thread pool for data loading
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            
            // Set up scheduled executor for progress reporting
            ScheduledExecutorService progressReporter = Executors.newSingleThreadScheduledExecutor();
            
            long startTime = System.currentTimeMillis();
            
            // Start the progress reporter to print updates every 30 seconds
            progressReporter.scheduleAtFixedRate(() -> {
                long currentTime = System.currentTimeMillis();
                double elapsedSeconds = (currentTime - startTime) / 1000.0;
                int currentCount = counter.get();
                
                if (currentCount > 0) {
                    logger.info("Progress: {} documents inserted ({} per second)", 
                        currentCount, String.format("%.2f", currentCount / elapsedSeconds));
                }
            }, 30, 30, TimeUnit.SECONDS);
            
            // Calculate documents per thread
            int docsPerThread = numDocuments / numThreads;
            int remainingDocs = numDocuments % numThreads;
            
            // Calculate duplicates per thread
            int dupesPerThread = duplicateCount / numThreads;
            int remainingDupes = duplicateCount % numThreads;
            
            // Pre-generate a set of ObjectIds that will be duplicated
            if (insertDupes && duplicateCount > 0) {
                for (int i = 0; i < duplicateCount; i++) {
                    duplicateIds.add(new ObjectId());
                }
                logger.info("Pre-generated {} ObjectIds for duplication", duplicateIds.size());
            }
            
            // Map out the shard keys for duplicates to ensure they land on different shards
            setupShardDistributionMap(duplicateCount);
            
            for (int i = 0; i < numThreads; i++) {
                int threadDocs = docsPerThread + (i == 0 ? remainingDocs : 0);
                int threadDupes = dupesPerThread + (i == 0 ? remainingDupes : 0);
                
                executor.submit(new DataLoader(
                    client, 
                    databaseName, 
                    collectionName, 
                    threadDocs,
                    threadDupes,
                    batchSize
                ));
            }
            
            // Shutdown executor and wait for completion
            executor.shutdown();
            boolean completed = executor.awaitTermination(1, TimeUnit.HOURS);
            
            // Shutdown the progress reporter
            progressReporter.shutdownNow();
            
            long endTime = System.currentTimeMillis();
            double duration = (endTime - startTime) / 1000.0;
            
            if (completed) {
                logger.info("Successfully inserted {} documents", counter.get());
                logger.info("Operation completed in {} seconds", String.format("%.2f", duration));
                logger.info("Insertion rate: {} docs/second", String.format("%.2f", counter.get() / duration));
                
                if (insertDupes) {
                    logger.info("Inserted {} documents with duplicate _ids", duplicateIds.size());
                }
            } else {
                logger.error("Timed out waiting for document insertion to complete");
                return 1;
            }
            
            return 0;
        } catch (Exception e) {
            logger.error("Error: {}", e.getMessage(), e);
            return 1;
        }
    }
    
    private void setupRangeSharding(MongoClient mongoClient, MongoDatabase database) {
        try {
            logger.info("Setting up range sharding for collection: {}", collectionName);
            
            // Enable sharding for the database if not already enabled
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document enableShardingResult = adminDb.runCommand(
                new Document("enableSharding", databaseName)
            );
            logger.info("Enable sharding result: {}", enableShardingResult.toJson());
            
            // Create an index on the shard key field 'x'
            database.getCollection(collectionName).createIndex(new Document("x", 1));
            logger.info("Created index on shard key field 'x'");
            
            // Shard the collection with range sharding on 'x'
            Document shardCmd = new Document("shardCollection", databaseName + "." + collectionName)
                .append("key", new Document("x", 1));
            
            Document shardResult = adminDb.runCommand(shardCmd);
            logger.info("Shard collection result: {}", shardResult.toJson());
            
            // Get available shards
            MongoDatabase configDb = mongoClient.getDatabase("config");
            MongoCollection<Document> shardsCollection = configDb.getCollection("shards");
           
            List<Document> shards = new ArrayList<>();
            shardsCollection.find().into(shards);
            
            if (shards.size() < 2) {
                logger.warn("Found only {} shard(s). At least 2 shards are recommended for duplicate distribution.", 
                    shards.size());
            }
            
            // Pre-split the chunks if we have multiple chunks defined
            if (numShardChunks > 1) {
                presplitChunks(adminDb, shards);
            }
            
        } catch (Exception e) {
            logger.warn("Failed to setup sharding: {}. Will continue with insertion anyway.", e.getMessage());
        }
    }
    
    private void presplitChunks(MongoDatabase adminDb, List<Document> shards) {
        try {
            logger.info("Pre-splitting collection into {} chunks", numShardChunks);
            
            // Calculate split points (we'll use integers spread evenly across the range)
            int maxValue = Integer.MAX_VALUE;
            int chunkSize = maxValue / numShardChunks;
            List<String> shardNames = new ArrayList<>();
            
            for (Document shard : shards) {
                shardNames.add(shard.getString("_id"));
            }
            
            // Split the chunks
            for (int i = 1; i < numShardChunks; i++) {
                int splitPoint = i * chunkSize;
                Document splitCmd = new Document("split", databaseName + "." + collectionName)
                    .append("middle", new Document("x", splitPoint));
                
                Document splitResult = adminDb.runCommand(splitCmd);
                logger.debug("Split result at x={}: {}", splitPoint, splitResult.toJson());
            }
            
            logger.info("Successfully pre-split collection into {} chunks", numShardChunks);
            
            // Manually distribute chunks in a round-robin fashion across shards
            if (shardNames.size() >= 2) {
                logger.info("Distributing chunks across {} shards", shardNames.size());
                distributeChunksInRoundRobin(adminDb, shardNames);
            }
        } catch (Exception e) {
            logger.warn("Failed to pre-split chunks: {}. Continuing with sharding anyway.", e.getMessage());
        }
    }
    
    private void distributeChunksInRoundRobin(MongoDatabase adminDb, List<String> shardNames) {
        try {
            // For each chunk (using chunk midpoints), move it to the appropriate shard in round-robin
            int maxValue = Integer.MAX_VALUE;
            int chunkSize = maxValue / numShardChunks;
            
            for (int i = 0; i < numShardChunks; i++) {
                // Calculate the midpoint of this chunk
                int midpoint = i * chunkSize + (chunkSize / 2);
                
                // Determine target shard in round-robin fashion
                String targetShard = shardNames.get(i % shardNames.size());
                
                // Create the moveChunk command
                Document moveCmd = new Document("moveChunk", databaseName + "." + collectionName)
                    .append("find", new Document("x", midpoint))
                    .append("to", targetShard);
                
                try {
                    Document moveResult = adminDb.runCommand(moveCmd);
                    logger.debug("Moved chunk with midpoint {} to shard {}: {}", 
                        midpoint, targetShard, moveResult.toJson());
                } catch (Exception e) {
                    logger.warn("Failed to move chunk with midpoint {} to shard {}: {}", 
                        midpoint, targetShard, e.getMessage());
                }
            }
            
            logger.info("Finished distributing chunks in round-robin fashion");
        } catch (Exception e) {
            logger.warn("Error in round-robin chunk distribution: {}", e.getMessage());
        }
    }
    
    private void setupShardDistributionMap(int duplicateCount) {
        // This map will help us distribute duplicate documents across different shards
        logger.info("Setting up shard distribution map for duplicate documents");
        
        try {
            // Get shard names
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            List<String> shardNames = new ArrayList<>();
            
            Document shardListResult = adminDb.runCommand(new Document("listShards", 1));
            List<Document> shards = (List<Document>) shardListResult.get("shards");
            
            if (shards.size() < 2) {
                logger.error("At least 2 shards are required for distributing duplicates");
                return;
            }
            
            for (Document shard : shards) {
                shardNames.add(shard.getString("_id"));
            }
            
            logger.info("Found {} shards for distributing duplicates: {}", shardNames.size(), shardNames);
            
            // Get current chunk distribution
            MongoDatabase configDb = mongoClient.getDatabase("config");
            MongoCollection<Document> collections = configDb.getCollection("collections");
            Document collectionsDoc = collections.find(eq("_id", databaseName + "." + collectionName)).first();
            Object uuid = collectionsDoc.get("uuid");
            
            List<Document> chunks = new ArrayList<>();
            MongoCollection<Document> chunksColl = configDb.getCollection("chunks");
            chunksColl.find(eq("uuid", uuid)).into(chunks);
            
            if (chunks == null || chunks.isEmpty()) {
                logger.warn("No chunks found for collection. Make sure sharding is properly set up.");
                return;
            }
            
            logger.info("Found {} chunks for collection {}.{}", chunks.size(), databaseName, collectionName);
            
            // Map chunks to their shards
            Map<String, List<Document>> chunksByShards = new HashMap<>();
            for (Document chunk : chunks) {
                String shardName = chunk.getString("shard");
                chunksByShards.computeIfAbsent(shardName, k -> new ArrayList<>()).add(chunk);
            }
            
            // For each duplicate ID, assign shard keys that will place it on different shards
            int index = 0;
            for (ObjectId id : duplicateIds) {
                // Select two different shards
                String shard1 = shardNames.get(index % shardNames.size());
                String shard2 = shardNames.get((index + 1) % shardNames.size());
                
                // Get chunks for these shards
                List<Document> chunksOnShard1 = chunksByShards.getOrDefault(shard1, new ArrayList<>());
                List<Document> chunksOnShard2 = chunksByShards.getOrDefault(shard2, new ArrayList<>());
                
                if (chunksOnShard1.isEmpty() || chunksOnShard2.isEmpty()) {
                    // Fallback to simple distribution if chunk info is not available
                    int chunkSize = Integer.MAX_VALUE / numShardChunks;
                    int value1 = (index % numShardChunks) * chunkSize + (chunkSize / 2);
                    int value2 = ((index + numShardChunks / 2) % numShardChunks) * chunkSize + (chunkSize / 2);
                    
                    shardDistributionMap.put(id.hashCode(), value1);
                    shardDistributionMap.put(-id.hashCode(), value2);
                    
                    logger.debug("Fallback: Mapped duplicate ID {} to shard keys {} and {}", 
                        id, value1, value2);
                } else {
                    // Get a chunk from each shard
                    Document chunk1 = chunksOnShard1.get(index % chunksOnShard1.size());
                    Document chunk2 = chunksOnShard2.get(index % chunksOnShard2.size());
                    
                    // Extract chunk boundaries
                    Document min1 = (Document) chunk1.get("min");
                    Document max1 = (Document) chunk1.get("max");
                    Document min2 = (Document) chunk2.get("min");
                    Document max2 = (Document) chunk2.get("max");
                    
                    // Choose shard key values in the middle of each chunk
                    int value1 = calculateMiddleValue(min1, max1);
                    int value2 = calculateMiddleValue(min2, max2);
                    
                    shardDistributionMap.put(id.hashCode(), value1);
                    shardDistributionMap.put(-id.hashCode(), value2);
                    
                    logger.debug("Mapped duplicate ID {} to shard keys {} and {} on shards {} and {}", 
                        id, value1, value2, shard1, shard2);
                }
                
                index++;
            }
            
            logger.info("Created shard distribution map for {} duplicate IDs", duplicateIds.size());
            
        } catch (Exception e) {
            logger.error("Error setting up shard distribution map", e);
            // Fall back to simple distribution method
            setupSimpleShardDistributionMap(duplicateCount);
        }
    }
    
    private int calculateMiddleValue(Document min, Document max) {
        // Get the shard key field 'x' from min and max documents
    	int minValue, maxValue;
    	Object minObj = min.get("x");
    	if (minObj instanceof Integer) {
    		minValue = (Integer)minObj;
    	} else if (minObj instanceof MinKey) {
    		minValue = Integer.MIN_VALUE;
    	} else {
    		throw new IllegalArgumentException("Unexpected x minKey value, type is " + minObj.getClass().getName());
    	}
    	
    	Object maxObj = max.get("x");
    	if (maxObj instanceof Integer) {
    		maxValue = (Integer)maxObj;
    	} else if (maxObj instanceof MaxKey) {
    		maxValue = Integer.MAX_VALUE;
    	} else {
    		throw new IllegalArgumentException("Unexpected x maxKey value, type is " + maxObj.getClass().getName());
    	}
         
        // Choose a value in the middle of the range
        if (maxValue == Integer.MAX_VALUE) {
            // For the last chunk, don't use Integer.MAX_VALUE directly
            return minValue + 1000000 + random.nextInt(1000000);
        }
        
        // For middle chunks, calculate middle with some randomness
        return minValue + (maxValue - minValue) / 2 + random.nextInt(Math.max(1, (maxValue - minValue) / 10));
    }
    
    private void setupSimpleShardDistributionMap(int duplicateCount) {
        logger.info("Using simple shard distribution as fallback");
        int chunkSize = Integer.MAX_VALUE / numShardChunks;
        
        int index = 0;
        for (ObjectId id : duplicateIds) {
            // Select one even and one odd chunk index to ensure different shards
            int chunkIndex1 = (index * 2) % numShardChunks;          // Will always be even
            int chunkIndex2 = (chunkIndex1 + 1) % numShardChunks;    // Will always be odd
            
            // Calculate values in the middle of different chunks
            int value1 = chunkIndex1 * chunkSize + (chunkSize / 2);
            int value2 = chunkIndex2 * chunkSize + (chunkSize / 2);
            
            // Store the mapping
            shardDistributionMap.put(id.hashCode(), value1);
            shardDistributionMap.put(-id.hashCode(), value2);
            
            logger.debug("Simple mapping: Duplicate ID {} to shard keys {} and {}", 
                id, value1, value2);
            
            index++;
        }
    }
    
    static class DataLoader implements Runnable {
        private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);
        
        private final MongoClient mongoClient;
        private final String databaseName;
        private final String collectionName;
        private final int documentsToInsert;
        private final int duplicatesToInsert;
        private final int batchSize;
        
        private final List<ObjectId> pendingDuplicateIds = new ArrayList<>();
        
        DataLoader(MongoClient mongoClient, String databaseName, String collectionName, 
                  int documentsToInsert, int duplicatesToInsert, int batchSize) {
            this.mongoClient = mongoClient;
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.documentsToInsert = documentsToInsert;
            this.duplicatesToInsert = duplicatesToInsert;
            this.batchSize = batchSize;
            
            // Claim a portion of the duplicate IDs for this thread
            synchronized (duplicateIds) {
                int count = 0;
                Iterator<ObjectId> iterator = duplicateIds.iterator();
                while (iterator.hasNext() && count < duplicatesToInsert) {
                    ObjectId id = iterator.next();
                    pendingDuplicateIds.add(id);
                    iterator.remove();
                    count++;
                }
                
                logger.debug("Thread claimed {} duplicate IDs for insertion", 
                    pendingDuplicateIds.size());
            }
        }
        
        @Override
        public void run() {
            MongoCollection<Document> collection = mongoClient
                .getDatabase(databaseName)
                .getCollection(collectionName);
                
            int inserted = 0;
            int batchCount = 0;
            
            try {
                // First insert regular documents
                int regularDocsToInsert = documentsToInsert - duplicatesToInsert;
                
                while (inserted < regularDocsToInsert) {
                    // Calculate batch size for this iteration
                    int currentBatchSize = Math.min(batchSize, regularDocsToInsert - inserted);
                    List<Document> batch = new ArrayList<>(currentBatchSize);
                    
                    // Generate batch of regular documents
                    for (int i = 0; i < currentBatchSize; i++) {
                        Document doc = generateDummyDocument(false, null);
                        batch.add(doc);
                    }
                    
                    // Insert batch
                    collection.insertMany(batch);
                    
                    inserted += currentBatchSize;
                    batchCount++;
                    counter.addAndGet(currentBatchSize);
                    
                    if (batchCount % 10 == 0) {
                        logger.debug("Thread {}: Inserted {} regular documents so far", 
                            Thread.currentThread().getName(), inserted);
                    }
                }
                
                // Now insert duplicate documents
                for (ObjectId duplicateId : pendingDuplicateIds) {
                    try {
                        // First document with this ID
                        Document doc1 = generateDummyDocument(true, duplicateId);
                        collection.insertOne(doc1);
                        counter.incrementAndGet();
                        
                        // Log that we're trying to insert a duplicate intentionally
                        logger.debug("Attempting to insert duplicate _id: {}", duplicateId);
                        
                        // Second document with the same ID (will be on different shard)
                        Document doc2 = generateDummyDocument(true, duplicateId);
                        // Use a negated hashcode to get different shard key for same ID
                        doc2.put("x", shardDistributionMap.getOrDefault(-duplicateId.hashCode(), random.nextInt()));
                        
                        try {
                            collection.insertOne(doc2);
                            counter.incrementAndGet();
                        } catch (Exception e) {
                            logger.warn("Duplicate _id rejected for: {} - {}", duplicateId, e.getMessage());
                            // Don't increment counter for documents that weren't inserted
                        }
                    } catch (Exception e) {
                        logger.warn("Error inserting duplicate document: {}", e.getMessage());
                    }
                }
                
                logger.info("Thread {} completed: Inserted {} total documents in {} batches", 
                    Thread.currentThread().getName(), inserted + pendingDuplicateIds.size(), batchCount);
                    
            } catch (Exception e) {
                logger.error("Error in thread {}: {}", Thread.currentThread().getName(), e.getMessage(), e);
            }
        }
        
        private Document generateDummyDocument(boolean isDuplicate, ObjectId specificId) {
            ObjectId docId;
            int docCounter = counter.get() + 1; // Use get() instead of incrementAndGet() to avoid double counting
            
            if (isDuplicate && specificId != null) {
                docId = specificId;
            } else {
                docId = new ObjectId();
            }
            
            // Store the ID in the generated set (for tracking)
            synchronized (generatedIds) {
                generatedIds.add(docId);
            }
            
            Document doc = new Document()
                .append("_id", docId)
                .append("value", "test-value-" + docCounter)
                .append("randomData", "Lorem ipsum dolor sit amet " + docCounter)
                .append("isActive", docCounter % 2 == 0)
                .append("score", docCounter * 1.5);
            
            if (isDuplicate) {
                doc.append("isDuplicate", true);
            }
            
            return doc;
        }
        
        public void verifyShardPlacement(ObjectId id, int shardKey1, int shardKey2) {
            try {
                MongoDatabase adminDb = mongoClient.getDatabase("admin");
                
                // Find which shard contains the first document
                Document explain1 = adminDb.runCommand(
                    new Document("explain", 
                        new Document("find", databaseName + "." + collectionName)
                        .append("filter", new Document("_id", id).append("x", shardKey1))
                    )
                    .append("verbosity", "queryPlanner")
                );
                
                // Find which shard contains the second document (should be a different shard)
                Document explain2 = adminDb.runCommand(
                    new Document("explain", 
                        new Document("find", databaseName + "." + collectionName)
                        .append("filter", new Document("_id", id).append("x", shardKey2))
                    )
                    .append("verbosity", "queryPlanner")
                );
                
                // Extract shard information from explain output
                String shard1 = extractShardFromExplain(explain1);
                String shard2 = extractShardFromExplain(explain2);
                
                if (shard1 != null && shard2 != null) {
                    if (shard1.equals(shard2)) {
                        logger.error("PROBLEM: Both documents with _id {} landed on same shard {}", 
                            id, shard1);
                    } else {
                        logger.info("SUCCESS: Documents with _id {} landed on different shards: {} and {}", 
                            id, shard1, shard2);
                    }
                } else {
                    logger.warn("Could not determine shard placement for one or both documents with _id {}", id);
                }
            } catch (Exception e) {
                logger.error("Error verifying shard placement: {}", e.getMessage());
            }
        }
        
        private String extractShardFromExplain(Document explain) {
            try {
                if (explain.containsKey("queryPlanner")) {
                    Document queryPlanner = (Document) explain.get("queryPlanner");
                    if (queryPlanner.containsKey("winningPlan")) {
                        Document winningPlan = (Document) queryPlanner.get("winningPlan");
                        if (winningPlan.containsKey("shards")) {
                            List<Document> shards = (List<Document>) winningPlan.get("shards");
                            if (!shards.isEmpty()) {
                                return shards.get(0).getString("shardName");
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Error extracting shard from explain: {}", e.getMessage());
            }
            return null;
        }
    }
}