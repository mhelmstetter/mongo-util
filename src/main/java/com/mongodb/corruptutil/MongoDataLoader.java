package com.mongodb.corruptutil;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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

@Command(name = "MongoDataLoader", mixinStandardHelpOptions = true, 
         description = "Loads dummy data into MongoDB collections with optional sharding")
public class MongoDataLoader implements Callable<Integer> {
    
    private static final Logger logger = LoggerFactory.getLogger(MongoDataLoader.class);

    @Option(names = {"-u", "--uri"}, description = "MongoDB connection URI", required = true)
    private String uri;

    @Option(names = {"-d", "--database"}, description = "Target database name", required = true)
    private String databaseName;

    @Option(names = {"-c", "--collection"}, description = "Target collection name", required = true)
    private String collectionName;

    @Option(names = {"-n", "--num-docs"}, description = "Number of documents to insert", defaultValue = "10000")
    private int numDocuments;

    @Option(names = {"-b", "--batch-size"}, description = "Batch size for inserts", defaultValue = "1000")
    private int batchSize;

    @Option(names = {"-t", "--threads"}, description = "Number of worker threads", defaultValue = "4")
    private int numThreads;

    @Option(names = {"-s", "--shard"}, description = "Enable sharding for the collection", defaultValue = "false")
    private boolean enableSharding;
    
    @Option(names = {"--insert-dupes"}, description = "Insert duplicate _id values (~1% of documents)", defaultValue = "true")
    private boolean insertDupes;
    
    @Option(names = {"--shard-chunks"}, description = "Number of chunks to pre-split", defaultValue = "16")
    private int numShardChunks;

    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final Random random = new Random();
    private static final Set<ObjectId> generatedIds = new HashSet<>();
    private static final Set<ObjectId> duplicateIds = new HashSet<>();
    private static final Map<Integer, Integer> shardDistributionMap = new HashMap<>();

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
        
        try (MongoClient mongoClient = MongoClients.create(mongoClientSettings)) {
            MongoDatabase database = mongoClient.getDatabase(databaseName);
            
            // Create the collection if it doesn't exist
            boolean collectionExists = database.listCollectionNames()
                    .into(new ArrayList<>())
                    .contains(collectionName);
                    
            if (!collectionExists) {
                logger.info("Creating collection: {}", collectionName);
                database.createCollection(collectionName);
            } else {
                // If collection exists and we're sharding, we should drop it first
                if (enableSharding) {
                    logger.info("Dropping existing collection to reconfigure sharding");
                    database.getCollection(collectionName).drop();
                    database.createCollection(collectionName);
                }
            }
            
            // Setup sharding if enabled
            if (enableSharding) {
                setupRangeSharding(mongoClient, database);
            }
            
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
            if (enableSharding && insertDupes) {
                setupShardDistributionMap(duplicateCount);
            }
            
            for (int i = 0; i < numThreads; i++) {
                int threadDocs = docsPerThread + (i == 0 ? remainingDocs : 0);
                int threadDupes = dupesPerThread + (i == 0 ? remainingDupes : 0);
                
                executor.submit(new DataLoader(
                    mongoClient, 
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
            
            // Pre-split the chunks
            if (numShardChunks > 1) {
                presplitChunks(adminDb);
            }
            
            // Shard the collection with range sharding on 'x'
            Document shardCmd = new Document("shardCollection", databaseName + "." + collectionName)
                .append("key", new Document("x", 1));
            
            Document shardResult = adminDb.runCommand(shardCmd);
            logger.info("Shard collection result: {}", shardResult.toJson());
        } catch (Exception e) {
            logger.warn("Failed to setup sharding: {}. Will continue with insertion anyway.", e.getMessage());
        }
    }
    
    private void presplitChunks(MongoDatabase adminDb) {
        try {
            logger.info("Pre-splitting collection into {} chunks", numShardChunks);
            
            // Calculate split points (we'll use integers spread evenly across the range)
            int maxValue = Integer.MAX_VALUE;
            int chunkSize = maxValue / numShardChunks;
            
            for (int i = 1; i < numShardChunks; i++) {
                int splitPoint = i * chunkSize;
                Document splitCmd = new Document("split", databaseName + "." + collectionName)
                    .append("middle", new Document("x", splitPoint));
                
                Document splitResult = adminDb.runCommand(splitCmd);
                logger.debug("Split result at x={}: {}", splitPoint, splitResult.toJson());
            }
            
            logger.info("Successfully pre-split collection into {} chunks", numShardChunks);
        } catch (Exception e) {
            logger.warn("Failed to pre-split chunks: {}. Continuing with sharding anyway.", e.getMessage());
        }
    }
    
    private void setupShardDistributionMap(int duplicateCount) {
        // This map will help us distribute duplicate documents across different shards
        // by assigning different shard key values for the same _id
        int chunkSize = Integer.MAX_VALUE / numShardChunks;
        
        int index = 0;
        for (ObjectId id : duplicateIds) {
            // For each duplicate ID, we'll assign two different shard keys
            // that fall into different chunks
            int shardIndex1 = index % numShardChunks;
            int shardIndex2 = (index + numShardChunks / 2) % numShardChunks;
            
            // Calculate values in the middle of different chunks
            int value1 = shardIndex1 * chunkSize + chunkSize / 2;
            int value2 = shardIndex2 * chunkSize + chunkSize / 2;
            
            // Store the mapping - we'll use the hash code of the ObjectId as key
            shardDistributionMap.put(id.hashCode(), value1);
            shardDistributionMap.put(-id.hashCode(), value2);  // Negated to make it different
            
            index++;
        }
        
        logger.info("Created shard distribution map for {} duplicate IDs", duplicateIds.size());
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
                for (ObjectId id : duplicateIds) {
                    if (count >= duplicatesToInsert) {
                        break;
                    }
                    pendingDuplicateIds.add(id);
                    count++;
                }
                // Remove the claimed IDs from the shared set
                duplicateIds.removeAll(pendingDuplicateIds);
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
                
                // Now insert duplicate documents (one by one to handle duplicates)
                for (ObjectId duplicateId : pendingDuplicateIds) {
                    try {
                        // First document with this ID
                        Document doc1 = generateDummyDocument(true, duplicateId);
                        collection.insertOne(doc1);
                        counter.incrementAndGet();
                        
                        // Second document with the same ID (will be on different shard)
                        Document doc2 = generateDummyDocument(true, duplicateId);
                        // Use a negated hashcode to get different shard key for same ID
                        doc2.put("x", shardDistributionMap.getOrDefault(-duplicateId.hashCode(), random.nextInt()));
                        
                        // Since this is a duplicate _id, it might fail, but that's expected
                        try {
                            collection.insertOne(doc2);
                            counter.incrementAndGet();
                        } catch (Exception e) {
                            // Expected duplicate key exception
                            logger.debug("Expected duplicate key error for _id: {}", duplicateId);
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
            int docCounter = counter.incrementAndGet();
            
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
                .append("x", isDuplicate ? 
                    shardDistributionMap.getOrDefault(docId.hashCode(), docCounter) : 
                    docCounter)
                .append("value", "test-value-" + docCounter)
                .append("randomData", "Lorem ipsum dolor sit amet " + docCounter)
                .append("isActive", docCounter % 2 == 0)
                .append("score", docCounter * 1.5);
            
            if (isDuplicate) {
                doc.append("isDuplicate", true);
            }
            
            return doc;
        }
    }
}