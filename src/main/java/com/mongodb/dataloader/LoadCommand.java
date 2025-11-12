package com.mongodb.dataloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "load",
         mixinStandardHelpOptions = true,
         description = "Load test data into MongoDB")
public class LoadCommand implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(LoadCommand.class);

    @Option(names = {"--uri"}, required = true, description = "MongoDB connection URI")
    private String uri;

    @Option(names = {"--numCollections"}, description = "Number of collections to create", defaultValue = "1")
    private int numCollections;

    @Option(names = {"--dataSize"}, split = ",",
            description = "Data size per collection. Format: 1MB, 1GB, 1TB. Multiple values for different collections. " +
                         "Can use plain numbers if --units is specified. Mutually exclusive with --shardSize.")
    private String[] dataSizes;

    @Option(names = {"--shardSize"}, split = ",",
            description = "Data size per shard. Format: 1MB, 1GB, 1TB. Multiple values for different shards. " +
                         "Can use plain numbers if --units is specified. Requires sharded cluster. Mutually exclusive with --dataSize.")
    private String[] shardSizes;

    @Option(names = {"--units"}, description = "Unit for data sizes (KB, MB, GB, TB). If specified, --dataSize/--shardSize values can be plain numbers.")
    private String units;

    @Option(names = {"--fields"}, description = "Number of fields per document", defaultValue = "10")
    private int fields;

    @Option(names = {"--threads"}, description = "Number of loader threads", defaultValue = "4")
    private int threads;

    @Option(names = {"--batchSize"}, description = "Documents per insertMany batch", defaultValue = "1024")
    private int batchSize;

    @Option(names = {"--database"}, description = "Database name", defaultValue = "testdb")
    private String databaseName;

    @Option(names = {"--collectionPrefix"}, description = "Collection name prefix", defaultValue = "testcoll")
    private String collectionPrefix;

    @Option(names = {"--drop"}, description = "Drop collection if it already exists before loading")
    private boolean dropCollection;

    private ShardClient shardClient;
    private MongoClient mongoClient;
    private boolean isSharded;
    private int numShards = 1;
    private AtomicLong totalDocsInserted = new AtomicLong(0);

    @Override
    public Integer call() throws Exception {
        logger.info("Starting data load");
        logger.info("URI: {}, Collections: {}, Threads: {}, BatchSize: {}",
                    uri, numCollections, threads, batchSize);

        try {
            // Validate that either dataSize or shardSize is provided (but not both)
            if ((dataSizes == null || dataSizes.length == 0) && (shardSizes == null || shardSizes.length == 0)) {
                throw new IllegalArgumentException("Either --dataSize or --shardSize must be specified");
            }
            if (dataSizes != null && dataSizes.length > 0 && shardSizes != null && shardSizes.length > 0) {
                throw new IllegalArgumentException("--dataSize and --shardSize are mutually exclusive");
            }

            // Initialize ShardClient and check if cluster is sharded
            initializeClient();

            if (shardSizes != null && shardSizes.length > 0) {
                // Shard-targeted loading mode
                if (!isSharded) {
                    throw new IllegalArgumentException("--shardSize requires a sharded cluster");
                }
                long[] parsedShardSizes = parseShardSizes();
                loadDataByShardSize(parsedShardSizes);
            } else {
                // Collection-based loading mode (original behavior)
                long[] parsedDataSizes = parseDataSizes();
                loadData(parsedDataSizes);
            }

            logger.info("Data loading completed. Total documents inserted: {}", totalDocsInserted.get());
            return 0;

        } catch (Exception e) {
            logger.error("Error during data loading", e);
            return 1;
        } finally {
            if (mongoClient != null) {
                mongoClient.close();
            }
        }
    }

    private void initializeClient() {
        logger.info("Connecting to MongoDB at: {}", uri);
        shardClient = new ShardClient("dataloader", uri);
        shardClient.init();
        mongoClient = shardClient.getMongoClient();

        isSharded = shardClient.isMongos();
        if (isSharded) {
            numShards = shardClient.getShardsMap().size();
            logger.info("Connected to sharded cluster with {} shards", numShards);
        } else {
            logger.info("Connected to non-sharded cluster (replica set or standalone)");
        }
    }

    private long[] parseDataSizes() {
        long[] sizes = new long[dataSizes.length];
        for (int i = 0; i < dataSizes.length; i++) {
            sizes[i] = parseDataSize(dataSizes[i]);
        }
        return sizes;
    }

    private long[] parseShardSizes() {
        long[] sizes = new long[shardSizes.length];
        for (int i = 0; i < shardSizes.length; i++) {
            sizes[i] = parseDataSize(shardSizes[i]);
        }
        return sizes;
    }

    private long parseDataSize(String sizeStr) {
        sizeStr = sizeStr.trim();
        String sizeStrUpper = sizeStr.toUpperCase();
        long multiplier;
        String numPart;

        // Check if the size has an embedded unit (e.g., "100MB")
        if (sizeStrUpper.endsWith("TB")) {
            multiplier = 1024L * 1024L * 1024L * 1024L;
            numPart = sizeStr.substring(0, sizeStr.length() - 2);
        } else if (sizeStrUpper.endsWith("GB")) {
            multiplier = 1024L * 1024L * 1024L;
            numPart = sizeStr.substring(0, sizeStr.length() - 2);
        } else if (sizeStrUpper.endsWith("MB")) {
            multiplier = 1024L * 1024L;
            numPart = sizeStr.substring(0, sizeStr.length() - 2);
        } else if (sizeStrUpper.endsWith("KB")) {
            multiplier = 1024L;
            numPart = sizeStr.substring(0, sizeStr.length() - 2);
        } else {
            // No embedded unit - check if global units parameter was specified
            if (units != null && !units.isEmpty()) {
                multiplier = getMultiplierForUnit(units);
                numPart = sizeStr;
            } else {
                // No unit specified at all - this is an error
                throw new IllegalArgumentException("Invalid data size format: '" + sizeStr +
                        "'. Unit is required. Either:\n" +
                        "  1. Include unit in size: --dataSize 100MB,5GB,1TB\n" +
                        "  2. Use --units parameter: --units GB --dataSize 3,5,10\n" +
                        "Examples: --dataSize 100MB or --units GB --dataSize 3,3,3");
            }
        }

        try {
            long size = Long.parseLong(numPart.trim());
            if (size < 0) {
                throw new IllegalArgumentException("Data size must be non-negative: " + sizeStr);
            }
            return size * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid data size format: '" + sizeStr +
                    "'. Expected numeric value. Examples: 100MB or --units GB --dataSize 3");
        }
    }

    private long getMultiplierForUnit(String unit) {
        String unitUpper = unit.trim().toUpperCase();
        switch (unitUpper) {
            case "TB":
                return 1024L * 1024L * 1024L * 1024L;
            case "GB":
                return 1024L * 1024L * 1024L;
            case "MB":
                return 1024L * 1024L;
            case "KB":
                return 1024L;
            default:
                throw new IllegalArgumentException("Invalid unit: '" + unit +
                        "'. Valid units are: KB, MB, GB, TB");
        }
    }

    private void loadData(long[] parsedDataSizes) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        try {
            for (int i = 0; i < numCollections; i++) {
                String collectionName = collectionPrefix + "_" + i;
                long targetSize = parsedDataSizes[i % parsedDataSizes.length];

                // Skip collections with 0 size
                if (targetSize == 0) {
                    logger.info("Collection {}: Skipping (target size is 0)", collectionName);
                    continue;
                }

                // If sharded, treat targetSize as per-shard size
                if (isSharded) {
                    targetSize = targetSize * numShards;
                    logger.info("Collection {}: target size {} bytes ({} bytes per shard across {} shards)",
                               collectionName, targetSize, targetSize / numShards, numShards);
                } else {
                    logger.info("Collection {}: target size {} bytes", collectionName, targetSize);
                }

                // Estimate document size and calculate number of documents needed
                int estimatedDocSize = estimateDocumentSize();
                long numDocuments = targetSize / estimatedDocSize;

                logger.info("Collection {}: Loading approximately {} documents (estimated {} bytes per doc)",
                           collectionName, numDocuments, estimatedDocSize);

                // Drop collection if requested
                MongoDatabase database = mongoClient.getDatabase(databaseName);
                if (dropCollection) {
                    logger.info("Collection {}: Dropping existing collection", collectionName);
                    database.getCollection(collectionName).drop();
                }

                // Create collection
                MongoCollection<Document> collection = database.getCollection(collectionName);

                // Submit loading tasks
                long docsPerThread = numDocuments / threads;
                for (int t = 0; t < threads; t++) {
                    final long startDoc = t * docsPerThread;
                    final long endDoc = (t == threads - 1) ? numDocuments : (t + 1) * docsPerThread;

                    executor.submit(new LoaderTask(collection, startDoc, endDoc, collectionName, t));
                }
            }

            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        } finally {
            executor.shutdownNow();
        }
    }

    private int estimateDocumentSize() {
        // Create a sample document with realistic values
        Document sample = generateDocument(100000); // Use larger doc_id for more realistic size

        // For shardSize mode, include the sk field in the estimate
        if (shardSizes != null && shardSizes.length > 0) {
            sample.append("sk", 5000); // Mid-range sk value
        }

        // JSON size is typically 30-40% larger than BSON for our data types
        // (ObjectId, dates, integers are more compact in BSON)
        // Use 65% of JSON size as a closer estimate to actual BSON size
        int jsonSize = sample.toJson().getBytes().length;
        return (int)(jsonSize * 0.65);
    }

    private Document generateDocument(long docId) {
        Document doc = new Document();
        Random random = new Random();

        for (int i = 0; i < fields; i++) {
            String fieldName = "field_" + i;
            Object value = generateFieldValue(i, random);
            doc.append(fieldName, value);
        }

        // Add a unique ID field
        doc.append("doc_id", docId);
        doc.append("created_at", new Date());

        return doc;
    }

    private Object generateFieldValue(int fieldIndex, Random random) {
        // Vary data types across fields
        int typeSelector = fieldIndex % 6;

        switch (typeSelector) {
            case 0:
                // ObjectId
                return new ObjectId();
            case 1:
                // UUID as string
                return UUID.randomUUID().toString();
            case 2:
                // Date
                return new Date(System.currentTimeMillis() - random.nextInt(365 * 24 * 60 * 60) * 1000L);
            case 3:
                // String (varying length)
                return generateRandomString(random, 20 + random.nextInt(80));
            case 4:
                // BinData
                byte[] bytes = new byte[16 + random.nextInt(48)];
                random.nextBytes(bytes);
                return new Binary(bytes);
            case 5:
                // Boolean
                return random.nextBoolean();
            default:
                return null;
        }
    }

    private String generateRandomString(Random random, int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private class LoaderTask implements Runnable {
        private final MongoCollection<Document> collection;
        private final long startDoc;
        private final long endDoc;
        private final String collectionName;
        private final int threadId;

        public LoaderTask(MongoCollection<Document> collection, long startDoc, long endDoc,
                         String collectionName, int threadId) {
            this.collection = collection;
            this.startDoc = startDoc;
            this.endDoc = endDoc;
            this.collectionName = collectionName;
            this.threadId = threadId;
        }

        @Override
        public void run() {
            try {
                logger.info("Thread {}: Loading documents {} to {} for collection {}",
                           threadId, startDoc, endDoc, collectionName);

                List<Document> batch = new ArrayList<>(batchSize);
                long docsInThread = 0;

                for (long docId = startDoc; docId < endDoc; docId++) {
                    batch.add(generateDocument(docId));

                    if (batch.size() >= batchSize) {
                        collection.insertMany(batch);
                        docsInThread += batch.size();
                        totalDocsInserted.addAndGet(batch.size());
                        batch.clear();

                        if (docsInThread % (batchSize * 10) == 0) {
                            logger.info("Thread {}: Inserted {} documents so far", threadId, docsInThread);
                        }
                    }
                }

                // Insert remaining documents
                if (!batch.isEmpty()) {
                    collection.insertMany(batch);
                    docsInThread += batch.size();
                    totalDocsInserted.addAndGet(batch.size());
                }

                logger.info("Thread {}: Completed. Inserted {} documents", threadId, docsInThread);

            } catch (Exception e) {
                logger.error("Thread {}: Error during data loading", threadId, e);
            }
        }
    }

    // Shard-targeted loading mode
    private void loadDataByShardSize(long[] parsedShardSizes) throws InterruptedException {
        logger.info("Starting shard-targeted loading mode");

        // Get list of shard names
        List<String> shardNames = new ArrayList<>(shardClient.getShardsMap().keySet());
        Collections.sort(shardNames);  // Ensure consistent ordering

        logger.info("Found {} shards: {}", shardNames.size(), shardNames);

        // Shard key range per shard (0-999 for shard0, 1000-1999 for shard1, etc.)
        int rangePerShard = 1000;

        for (int collIdx = 0; collIdx < numCollections; collIdx++) {
            String collectionName = collectionPrefix + "_" + collIdx;
            String namespace = databaseName + "." + collectionName;

            logger.info("Setting up collection: {}", collectionName);

            MongoDatabase database = mongoClient.getDatabase(databaseName);

            // Drop collection if requested
            if (dropCollection) {
                logger.info("Collection {}: Dropping existing collection", collectionName);
                database.getCollection(collectionName).drop();
            }

            MongoCollection<Document> collection = database.getCollection(collectionName);

            // Shard the collection with ranged shard key
            Document shardKey = new Document("sk", 1);  // "sk" = shard key field
            logger.info("Sharding collection {} with key: {}", namespace, shardKey);
            shardClient.shardCollection(new com.mongodb.model.Namespace(namespace), shardKey);

            // Pre-split collection into chunks
            logger.info("Pre-splitting collection {} into {} chunks", collectionName, shardNames.size());

            // Step 1: Do all splits first
            for (int i = 1; i < shardNames.size(); i++) {
                int splitSk = i * rangePerShard;
                logger.debug("Splitting at sk={}", splitSk);
                shardClient.splitAt(namespace, new org.bson.BsonDocument("sk", new org.bson.BsonInt32(splitSk)), false);
            }

            // Step 2: Move all chunks to target shards
            logger.info("Distributing chunks to shards");
            for (int i = 0; i < shardNames.size(); i++) {
                String shardName = shardNames.get(i);
                int minSk = i * rangePerShard;

                // For the first chunk, use MinKey as lower bound
                org.bson.BsonDocument minBound;
                if (i == 0) {
                    minBound = new org.bson.BsonDocument("sk", new org.bson.BsonMinKey());
                } else {
                    minBound = new org.bson.BsonDocument("sk", new org.bson.BsonInt32(minSk));
                }

                int maxSk = (i + 1) * rangePerShard;
                org.bson.BsonDocument maxBound;
                if (i == shardNames.size() - 1) {
                    // Last chunk goes to MaxKey
                    maxBound = new org.bson.BsonDocument("sk", new org.bson.BsonMaxKey());
                } else {
                    maxBound = new org.bson.BsonDocument("sk", new org.bson.BsonInt32(maxSk));
                }

                logger.debug("Moving chunk [{}  to {}) to shard {}",
                            i == 0 ? "MinKey" : "sk=" + minSk,
                            i == shardNames.size() - 1 ? "MaxKey" : "sk=" + maxSk,
                            shardName);
                shardClient.moveChunk(
                    namespace,
                    minBound,
                    maxBound,
                    shardName,
                    false, // ignoreMissing
                    false, // secondaryThrottle
                    false, // waitForDelete
                    false, // majorityWrite
                    false, // throwCommandExceptions
                    false  // useMoveRange
                );
            }

            // Create one thread per shard
            ExecutorService executor = Executors.newFixedThreadPool(shardNames.size());

            try {
                for (int shardIdx = 0; shardIdx < shardNames.size(); shardIdx++) {
                    long targetSize = parsedShardSizes[shardIdx % parsedShardSizes.length];
                    String shardName = shardNames.get(shardIdx);

                    if (targetSize == 0) {
                        logger.info("Collection {}, Shard {}: Skipping (target size is 0)", collectionName, shardName);
                        continue;
                    }

                    logger.info("Collection {}, Shard {}: target size {} bytes", collectionName, shardName, targetSize);

                    // Estimate document size and calculate number of documents
                    int estimatedDocSize = estimateDocumentSize();
                    long numDocuments = targetSize / estimatedDocSize;

                    logger.info("Collection {}, Shard {}: Loading approximately {} documents (estimated {} bytes per doc)",
                               collectionName, shardName, numDocuments, estimatedDocSize);

                    // Calculate shard key range for this shard
                    int minSk = shardIdx * rangePerShard;
                    int maxSk = (shardIdx + 1) * rangePerShard;

                    executor.submit(new ShardLoaderTask(collection, numDocuments, minSk, maxSk, collectionName, shardName));
                }

                executor.shutdown();
                executor.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

            } finally {
                executor.shutdownNow();
            }
        }
    }

    private class ShardLoaderTask implements Runnable {
        private final MongoCollection<Document> collection;
        private final long numDocuments;
        private final int minSk;
        private final int maxSk;
        private final String collectionName;
        private final String shardName;

        public ShardLoaderTask(MongoCollection<Document> collection, long numDocuments,
                               int minSk, int maxSk, String collectionName, String shardName) {
            this.collection = collection;
            this.numDocuments = numDocuments;
            this.minSk = minSk;
            this.maxSk = maxSk;
            this.collectionName = collectionName;
            this.shardName = shardName;
        }

        @Override
        public void run() {
            try {
                logger.info("Shard {}: Loading {} documents with sk range [{}, {})",
                           shardName, numDocuments, minSk, maxSk);

                List<Document> batch = new ArrayList<>(batchSize);
                long docsLoaded = 0;
                Random random = new Random();

                for (long docId = 0; docId < numDocuments; docId++) {
                    // Generate shard key value within this shard's range
                    int skValue = minSk + random.nextInt(maxSk - minSk);

                    Document doc = generateDocument(docId);
                    doc.append("sk", skValue);  // Add shard key field
                    batch.add(doc);

                    if (batch.size() >= batchSize) {
                        collection.insertMany(batch);
                        docsLoaded += batch.size();
                        totalDocsInserted.addAndGet(batch.size());
                        batch.clear();

                        if (docsLoaded % (batchSize * 10) == 0) {
                            logger.info("Shard {}: Inserted {} documents so far", shardName, docsLoaded);
                        }
                    }
                }

                // Insert remaining documents
                if (!batch.isEmpty()) {
                    collection.insertMany(batch);
                    docsLoaded += batch.size();
                    totalDocsInserted.addAndGet(batch.size());
                }

                logger.info("Shard {}: Completed. Inserted {} documents", shardName, docsLoaded);

            } catch (Exception e) {
                logger.error("Shard {}: Error during data loading", shardName, e);
            }
        }
    }
}
