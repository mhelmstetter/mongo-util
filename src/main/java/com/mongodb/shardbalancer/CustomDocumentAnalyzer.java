package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.or;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Sorts.descending;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonValueWrapper;
import com.mongodb.model.Namespace;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "customAnalyzer", mixinStandardHelpOptions = true, version = "customAnalyzer 0.1", 
         description = "Custom document analyzer for specific shard and date patterns")
public class CustomDocumentAnalyzer extends Balancer implements Callable<Integer> {
    
    @Option(names = {"--shardIndex"}, required = true, description = "Index of the shard to query (0-based)")
    private int shardIndex;
    
    @Option(names = {"--yearMonth"}, required = true, description = "Year and month pattern (e.g., 202508)")
    private String yearMonth;
    
    @Option(names = {"--ns"}, required = true, description = "Namespace in format db.collection")
    private String namespace;
    
    @Option(names = {"--init"}, description = "Run initialization phase")
    private boolean initMode = false;
    
    @Option(names = {"--balance"}, description = "Run balance phase")
    private boolean balanceMode = false;
    
    @Option(names = {"--destShardIndex"}, description = "Destination shard indexes for balancing (comma-separated)")
    private String destShardIndexes;
    
    @Option(names = {"--dryRun"}, description = "Dry run mode - show what would be moved without actually moving")
    private boolean dryRun = false;
    
    @Option(names = {"--migrate"}, description = "Migrate existing stats collection to add chunkMin/chunkMax fields")
    private boolean migrateMode = false;
    
    @Option(names = {"--chunkLimit"}, description = "Maximum number of chunks to move during balance phase (0 = no limit)")
    private int chunkLimit = 0;

    private final Logger logger = LoggerFactory.getLogger(CustomDocumentAnalyzer.class);
    
    private final static String SOURCE_URI = "source";
    private final static int BATCH_SIZE = 1000;
    
    // Status tracking
    private int successfulMoves = 0;
    private int totalChunksMoved = 0; // Tracks actual physical chunks moved (including all splits)
    private int splitOperations = 0;
    private int chunksProcessed = 0;
    private long startTime = 0;
    private long lastStatusReport = 0;
    private final static int STATUS_REPORT_INTERVAL_MS = 60000; // Report every 60 seconds
    
    private BalancerConfig balancerConfig;
    
    private void reportStatus() {
        long currentTime = System.currentTimeMillis();
        if (currentTime - lastStatusReport >= STATUS_REPORT_INTERVAL_MS) {
            long elapsedMinutes = (currentTime - startTime) / 60000;
            double movesPerMinute = elapsedMinutes > 0 ? (double) successfulMoves / elapsedMinutes : 0;
            
            String statusMessage;
            if (chunkLimit > 0) {
                statusMessage = String.format("📊 STATUS: %d chunks processed, %d/%d successful moves (%d total chunks moved), %d splits | " +
                               "Runtime: %dm | Rate: %s moves/min", 
                               chunksProcessed, successfulMoves, chunkLimit, totalChunksMoved, splitOperations, elapsedMinutes, 
                               String.format("%.1f", movesPerMinute));
            } else {
                statusMessage = String.format("📊 STATUS: %d chunks processed, %d successful moves (%d total chunks moved), %d splits | " +
                               "Runtime: %dm | Rate: %s moves/min", 
                               chunksProcessed, successfulMoves, totalChunksMoved, splitOperations, elapsedMinutes, 
                               String.format("%.1f", movesPerMinute));
            }
            logger.info(statusMessage);
            lastStatusReport = currentTime;
        }
    }
    
    @Override
    protected void onChunkSplit() {
        splitOperations++;
    }
    
    @Override
    protected void onChunkMoved() {
        totalChunksMoved++;
    }

    @Override
    public Integer call() throws ConfigurationException {
        
        if (!initMode && !balanceMode && !migrateMode) {
            logger.error("Must specify one of: --init, --balance, or --migrate mode");
            return 1;
        }
        
        int modeCount = (initMode ? 1 : 0) + (balanceMode ? 1 : 0) + (migrateMode ? 1 : 0);
        if (modeCount > 1) {
            logger.error("Cannot specify multiple modes. Choose one of: --init, --balance, or --migrate");
            return 1;
        }
        
        parseArgs();
        init();
        
        if (initMode) {
            runInitialization();
        } else if (balanceMode) {
            runBalance();
        } else if (migrateMode) {
            runMigration();
        }
        
        return 0;
    }
    
    protected void parseArgs() throws ConfigurationException {
        Configuration config = readProperties();
        this.balancerConfig = new BalancerConfig();
        balancerConfig.setSourceClusterUri(config.getString(SOURCE_URI));
        
        // Validate namespace format
        if (!namespace.contains(".")) {
            throw new IllegalArgumentException("Namespace must be in format db.collection, got: " + namespace);
        }
        
        // Validate yearMonth format (should be 6 digits)
        if (!yearMonth.matches("\\d{6}")) {
            throw new IllegalArgumentException("yearMonth must be 6 digits (YYYYMM), got: " + yearMonth);
        }
        
        logger.info("Configuration: shardIndex={}, yearMonth={}, namespace={}", shardIndex, yearMonth, namespace);
    }
    
    public void init() {
        sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri());
        sourceShardClient.init();
        sourceShardClient.populateShardMongoClients();
        balancerConfig.setSourceShardClient(sourceShardClient);
        
        // Initialize stats collection
        balancerConfig.setStatsCollection(sourceShardClient.getCollectionBson(balancerConfig.getStatsNamespace()));
        
        chunkManager = new ChunkManager(balancerConfig);
        chunkManager.setSourceShardClient(sourceShardClient);
        chunkManager.initializeSourceChunkQuery();
        chunkMap = new HashMap<>();
        sourceChunksCache = new LinkedHashMap<>();
        
        // Load chunk map for the specified namespace only
        logger.info("Loading chunk map for namespace: {}", namespace);
        chunkManager.loadChunkMap(namespace, sourceChunksCache, chunkMap);
        
        NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap = chunkMap.get(namespace);
        if (nsChunkMap == null || nsChunkMap.isEmpty()) {
            throw new RuntimeException("No chunks found for namespace: " + namespace + ". Is it sharded?");
        }
        
        logger.info("Loaded {} chunks for namespace {}", nsChunkMap.size(), namespace);
    }
    
    private void runInitialization() {
        
        // Parse namespace
        String[] parts = namespace.split("\\.", 2);
        String dbName = parts[0];
        String collectionName = parts[1];
        
        logger.info("Starting initialization phase for database: {}, collection: {}", dbName, collectionName);
        
        // Get list of shards and validate shardIndex
        List<String> shardIds = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
        if (shardIndex >= shardIds.size()) {
            throw new IllegalArgumentException("shardIndex " + shardIndex + " is out of range. Available shards: " + shardIds.size());
        }
        
        String targetShardId = shardIds.get(shardIndex);
        logger.info("Targeting shard: {} (index {})", targetShardId, shardIndex);
        
        // Get the specific shard's mongo client
        MongoCollection<RawBsonDocument> collection = sourceShardClient.getShardMongoClient(targetShardId)
                .getDatabase(dbName)
                .getCollection(collectionName, RawBsonDocument.class);
        
        // Create regex pattern for _id matching
        String regexPattern = ".*:" + yearMonth + "$";
        Pattern pattern = Pattern.compile(regexPattern);
        logger.info("Querying for documents with _id matching pattern: {}", regexPattern);
        
        // Get chunk map for namespace
        NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap = chunkMap.get(namespace);
        
        // Prepare batch processing
        List<WriteModel<BsonDocument>> writeModels = new ArrayList<>();
        long totalDocuments = 0;
        long totalBsonSize = 0;
        
        // Query documents matching the regex
        try (MongoCursor<RawBsonDocument> cursor = collection.find(regex("_id", regexPattern)).iterator()) {
            
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                
                // Calculate BSON size
                int bsonSize = doc.getByteBuffer().remaining();
                totalBsonSize += bsonSize;
                
                // Get _id for chunk mapping
                Object docId = doc.get("_id");
                BsonValueWrapper idWrapper = new BsonValueWrapper(doc.get("_id"));
                
                // Find which chunk this document belongs to
                Map.Entry<BsonValueWrapper, CountingMegachunk> chunkEntry = nsChunkMap.floorEntry(idWrapper);
                String chunkId = null;
                if (chunkEntry != null) {
                    CountingMegachunk chunk = chunkEntry.getValue();
                    chunkId = chunk.getId(); // This will be the chunk identifier
                }
                
                // Create document for stats collection
                BsonDocument statsDoc = new BsonDocument();
                statsDoc.append("_id", doc.get("_id"));
                statsDoc.append("bsonSize", new org.bson.BsonInt32(bsonSize));
                statsDoc.append("namespace", new org.bson.BsonString(namespace));
                statsDoc.append("yearMonth", new org.bson.BsonString(yearMonth));
                statsDoc.append("shardId", new org.bson.BsonString(targetShardId));
                if (chunkId != null) {
                    statsDoc.append("chunkId", new org.bson.BsonString(chunkId));
                }
                if (chunkEntry != null) {
                    CountingMegachunk chunk = chunkEntry.getValue();
                    // Store min/max bounds for efficient lookup during balance phase
                    statsDoc.append("chunkMin", chunk.getMin());
                    statsDoc.append("chunkMax", chunk.getMax());
                }
                
                writeModels.add(new InsertOneModel<>(statsDoc));
                totalDocuments++;
                
                // Batch insert when we reach batch size
                if (writeModels.size() >= BATCH_SIZE) {
                    insertBatch(writeModels);
                    writeModels.clear();
                }
                
                if (totalDocuments % 5000 == 0) {
                    logger.info("Processed {} documents, total BSON size: {} bytes", totalDocuments, totalBsonSize);
                }
            }
            
            // Insert remaining documents
            if (!writeModels.isEmpty()) {
                insertBatch(writeModels);
            }
            
            logger.info("Initialization phase completed. Total documents: {}, total BSON size: {} bytes, average size: {} bytes", 
                       totalDocuments, totalBsonSize, totalDocuments > 0 ? totalBsonSize / totalDocuments : 0);
        }
    }
    
    private void runBalance() {
        // Parse namespace for moveChunk operations
        String[] parts = namespace.split("\\.", 2);
        if (parts.length != 2) {
            logger.error("Invalid namespace format: {}. Expected format: db.collection", namespace);
            return;
        }
        String dbName = parts[0];
        String collectionName = parts[1];
        Namespace ns = new Namespace(dbName, collectionName);
        
        if (chunkLimit > 0) {
            logger.info("Starting balance phase for namespace: {} (chunk limit: {})", namespace, chunkLimit);
        } else {
            logger.info("Starting balance phase for namespace: {} (no chunk limit)", namespace);
        }
        
        // Initialize status tracking
        startTime = System.currentTimeMillis();
        lastStatusReport = startTime;
        successfulMoves = 0;
        totalChunksMoved = 0;
        splitOperations = 0;
        chunksProcessed = 0;
        
        // Check if dry run mode
        boolean isDryRun = dryRun || (destShardIndexes == null || destShardIndexes.trim().isEmpty());
        
        if (isDryRun && !dryRun && (destShardIndexes == null || destShardIndexes.trim().isEmpty())) {
            logger.info("No --destShardIndex provided and --dryRun not specified. Running in dry run mode by default.");
        }
        
        // Parse destination shard indexes if provided
        List<Integer> destShardIndexList = new ArrayList<>();
        
        if (!isDryRun) {
            try {
                String[] indexes = destShardIndexes.split(",");
                for (String index : indexes) {
                    destShardIndexList.add(Integer.parseInt(index.trim()));
                }
                logger.info("Destination shard indexes: {}", destShardIndexList);
            } catch (NumberFormatException e) {
                logger.error("Invalid destination shard indexes: {}", destShardIndexes);
                return;
            }
        } else {
            logger.info("Running in dry run mode - no chunks will be moved");
        }
        
        // Get list of shards for validation
        List<String> shardIds = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
        
        // Validate destination shard indexes
        if (!isDryRun) {
            for (Integer index : destShardIndexList) {
                if (index >= shardIds.size()) {
                    logger.error("Destination shard index {} is out of range. Available shards: {}", index, shardIds.size());
                    return;
                }
            }
        }
        
        // Build aggregation pipeline
        List<org.bson.conversions.Bson> pipeline = new ArrayList<>();
        
        // Match documents with bsonSize >= 1000000 and (move:false OR move field doesn't exist)
        pipeline.add(match(and(
            gte("bsonSize", 1000000),
            or(eq("move", false), exists("move", false))
        )));
        
        // Group by chunkMin and count (using chunkMin as unique identifier)
        pipeline.add(group("$chunkMin", sum("count", 1)));
        
        // Sort by count descending
        pipeline.add(sort(descending("count")));
        
        // Limit to 100 for dry run
        if (isDryRun) {
            pipeline.add(limit(100));
        }
        
        MongoCollection<BsonDocument> statsCollection = balancerConfig.getStatsCollection();
        AggregateIterable<Document> results = statsCollection.aggregate(pipeline, Document.class);
        
        int roundRobinIndex = 0;
        int processedCount = 0;
        
        try (MongoCursor<Document> cursor = results.iterator()) {
            while (cursor.hasNext()) {
                Document result = cursor.next();
                BsonDocument chunkMinDoc = ((Document) result.get("_id")).toBsonDocument();
                // Extract the _id value to match how chunk map keys are stored  
                BsonValue chunkMinValue = chunkMinDoc.get("_id");
                int count = result.getInteger("count");
                
                if (isDryRun) {
                    logger.info("Chunk min: {}, Count: {}", chunkMinDoc, count);
                    processedCount++;
                    continue;
                }
                
                // Get destination shard
                int destShardIndex = destShardIndexList.get(roundRobinIndex % destShardIndexList.size());
                String destShardId = shardIds.get(destShardIndex);
                
                chunksProcessed++;
                // Moving chunk to destination shard
                reportStatus();
                
                // Efficiently find the chunk using the min bound as the key
                NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap = chunkMap.get(namespace);
                if (nsChunkMap == null) {
                    logger.error("No chunk map found for namespace: {}", namespace);
                    continue;
                }
                
                BsonValueWrapper minWrapper = new BsonValueWrapper(chunkMinValue);
                CountingMegachunk chunk = nsChunkMap.get(minWrapper);
                
                if (chunk == null) {
                    logger.warn("Could not find chunk with exact min {}, trying floorEntry", chunkMinValue);
                    Map.Entry<BsonValueWrapper, CountingMegachunk> entry = nsChunkMap.floorEntry(minWrapper);
                    if (entry != null) {
                        chunk = entry.getValue();
                        logger.debug("Found chunk via floorEntry: min={}, max={}", chunk.getMin(), chunk.getMax());
                    }
                }
                
                if (chunk == null) {
                    logger.error("Could not find chunk with min {} in chunk map", chunkMinValue);
                    continue;
                }
                
                // Perform moveChunk operation with retry logic
                boolean success = moveChunkWithRetry(namespace, chunk, destShardId, 10);
                
                if (success) {
                    successfulMoves++;
                    // Successfully moved chunk
                    
                    // Check if we've reached the chunk limit
                    if (chunkLimit > 0 && successfulMoves >= chunkLimit) {
                        logger.info("Reached chunk limit of {} successful moves, stopping balance phase", chunkLimit);
                        break;
                    }
                    
                    // Update stats collection with move:true for this chunkMin
                    try {
                        UpdateResult updateResult = statsCollection.updateMany(
                            eq("chunkMin", chunkMinDoc),
                            set("move", true)
                        );
                        if (updateResult.getMatchedCount() != updateResult.getModifiedCount()) {
                            logger.warn("Stats update mismatch for chunk {}: matched={}, modified={}", 
                                       chunkMinDoc, updateResult.getMatchedCount(), updateResult.getModifiedCount());
                        }
                    } catch (Exception e) {
                        logger.warn("Failed to update stats collection for chunk with min {}: {}", chunkMinDoc, e.getMessage());
                    }
                } else {
                    logger.error("Failed to move chunk with min {} to shard {}", chunkMinDoc, destShardId);
                }
                
                roundRobinIndex++;
                processedCount++;
            }
        }
        
        if (isDryRun) {
            logger.info("Dry run completed. Showed {} chunks that would be moved", processedCount);
        } else {
            // Final status report
            long totalElapsed = (System.currentTimeMillis() - startTime) / 60000;
            double finalRate = totalElapsed > 0 ? (double) successfulMoves / totalElapsed : 0;
            
            String finalStatusMessage;
            if (chunkLimit > 0) {
                String limitStatus = successfulMoves >= chunkLimit ? "LIMIT REACHED" : "INCOMPLETE";
                finalStatusMessage = String.format("🏁 FINAL STATUS [%s]: %d chunks processed, %d/%d successful moves (%d total chunks moved), %d splits | " +
                                   "Total runtime: %dm | Final rate: %s moves/min", 
                                   limitStatus, chunksProcessed, successfulMoves, chunkLimit, totalChunksMoved, splitOperations, totalElapsed, 
                                   String.format("%.1f", finalRate));
            } else {
                finalStatusMessage = String.format("🏁 FINAL STATUS: %d chunks processed, %d successful moves (%d total chunks moved), %d splits | " +
                                   "Total runtime: %dm | Final rate: %s moves/min", 
                                   chunksProcessed, successfulMoves, totalChunksMoved, splitOperations, totalElapsed, 
                                   String.format("%.1f", finalRate));
            }
            logger.info(finalStatusMessage);
        }
    }
    
    private void runMigration() {
        logger.info("Starting migration of stats collection for namespace: {}", namespace);
        
        MongoCollection<BsonDocument> statsCollection = balancerConfig.getStatsCollection();
        NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap = chunkMap.get(namespace);
        
        if (nsChunkMap == null) {
            logger.error("No chunk map loaded for namespace: {}. Migration requires chunk data.", namespace);
            return;
        }
        
        // Find documents that need migration (have chunkId but missing chunkMin/chunkMax)
        BsonDocument migrationQuery = new BsonDocument()
            .append("namespace", new org.bson.BsonString(namespace))
            .append("chunkId", new BsonDocument("$exists", new org.bson.BsonBoolean(true)))
            .append("chunkMin", new BsonDocument("$exists", new org.bson.BsonBoolean(false)));
        
        logger.info("Querying for documents that need migration...");
        long totalDocuments = statsCollection.countDocuments(migrationQuery);
        logger.info("Found {} documents that need migration", totalDocuments);
        
        if (totalDocuments == 0) {
            logger.info("No documents need migration. All documents already have chunkMin/chunkMax fields.");
            return;
        }
        
        List<WriteModel<BsonDocument>> bulkOps = new ArrayList<>();
        long processedCount = 0;
        long migratedCount = 0;
        long skippedCount = 0;
        
        try (MongoCursor<BsonDocument> cursor = statsCollection.find(migrationQuery).iterator()) {
            while (cursor.hasNext()) {
                BsonDocument doc = cursor.next();
                String chunkId = doc.getString("chunkId").getValue();
                
                // Find the chunk by its ID in our chunk map
                CountingMegachunk chunk = findChunkById(nsChunkMap, chunkId);
                
                if (chunk != null) {
                    // Create update operation to add chunkMin/chunkMax and remove chunkId
                    BsonDocument filter = new BsonDocument("_id", doc.get("_id"));
                    BsonDocument update = new BsonDocument("$set", new BsonDocument()
                        .append("chunkMin", chunk.getMin())
                        .append("chunkMax", chunk.getMax()))
                        .append("$unset", new BsonDocument("chunkId", new org.bson.BsonString("")));
                    
                    bulkOps.add(new UpdateOneModel<>(filter, update));
                    migratedCount++;
                } else {
                    logger.warn("Could not find chunk with ID: {} in chunk map. Skipping document.", chunkId);
                    skippedCount++;
                }
                
                processedCount++;
                
                // Execute batch when it reaches batch size
                if (bulkOps.size() >= BATCH_SIZE) {
                    executeMigrationBatch(statsCollection, bulkOps);
                    bulkOps.clear();
                }
                
                if (processedCount % 5000 == 0) {
                    logger.info("Processed {} documents, migrated: {}, skipped: {}", 
                               processedCount, migratedCount, skippedCount);
                }
            }
            
            // Execute remaining operations
            if (!bulkOps.isEmpty()) {
                executeMigrationBatch(statsCollection, bulkOps);
            }
        }
        
        logger.info("Migration completed. Processed: {}, migrated: {}, skipped: {}", 
                   processedCount, migratedCount, skippedCount);
        
        // Verify migration
        long remainingDocuments = statsCollection.countDocuments(migrationQuery);
        logger.info("Verification: {} documents still need migration", remainingDocuments);
        
        if (remainingDocuments == 0) {
            logger.info("✅ Migration successful! All documents now have chunkMin/chunkMax fields.");
        } else {
            logger.warn("⚠️ Migration incomplete. {} documents still need migration.", remainingDocuments);
        }
    }
    
    private CountingMegachunk findChunkById(NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap, String chunkId) {
        // Search through the chunk map to find a chunk with matching ID
        for (CountingMegachunk chunk : nsChunkMap.values()) {
            if (chunkId.equals(chunk.getId())) {
                return chunk;
            }
        }
        return null;
    }
    
    private void executeMigrationBatch(MongoCollection<BsonDocument> statsCollection, List<WriteModel<BsonDocument>> bulkOps) {
        try {
            BulkWriteResult result = statsCollection.bulkWrite(bulkOps, new BulkWriteOptions().ordered(false));
            logger.debug("Migration batch completed. Modified: {}", result.getModifiedCount());
        } catch (MongoBulkWriteException e) {
            logger.warn("Migration batch partial failure. Modified: {}, errors: {}", 
                       e.getWriteResult().getModifiedCount(), e.getWriteErrors().size());
            // Log first few errors for debugging
            for (int i = 0; i < Math.min(3, e.getWriteErrors().size()); i++) {
                logger.warn("Migration error {}: {}", i + 1, e.getWriteErrors().get(i).getMessage());
            }
        } catch (Exception e) {
            logger.error("Migration batch failed: {}", e.getMessage());
            throw e;
        }
    }
    
    private void insertBatch(List<WriteModel<BsonDocument>> writeModels) {
        MongoCollection<BsonDocument> statsCollection = balancerConfig.getStatsCollection();
        
        try {
            BulkWriteResult result = statsCollection.bulkWrite(writeModels, new BulkWriteOptions().ordered(false));
            logger.debug("Inserted batch of {} documents", result.getInsertedCount());
        } catch (MongoBulkWriteException e) {
            logger.warn("Bulk write partial failure. Inserted: {}, errors: {}", 
                       e.getWriteResult().getInsertedCount(), e.getWriteErrors().size());
            // Continue processing despite errors
        }
    }
    
    private Configuration readProperties() throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<PropertiesConfiguration>(
                PropertiesConfiguration.class).configure(params.properties().setFileName(configFile)
                        .setListDelimiterHandler(new DefaultListDelimiterHandler(',')));
        return builder.getConfiguration();
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new CustomDocumentAnalyzer()).execute(args);
        System.exit(exitCode);
    }
}