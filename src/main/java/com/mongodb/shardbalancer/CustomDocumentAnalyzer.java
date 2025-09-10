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

    private final Logger logger = LoggerFactory.getLogger(CustomDocumentAnalyzer.class);
    
    private final static String SOURCE_URI = "source";
    private final static int BATCH_SIZE = 1000;
    
    private BalancerConfig balancerConfig;

    @Override
    public Integer call() throws ConfigurationException {
        
        if (!initMode && !balanceMode) {
            logger.error("Must specify either --init or --balance mode");
            return 1;
        }
        
        if (initMode && balanceMode) {
            logger.error("Cannot specify both --init and --balance modes");
            return 1;
        }
        
        parseArgs();
        init();
        
        if (initMode) {
            runInitialization();
        } else if (balanceMode) {
            runBalance();
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
                
                writeModels.add(new InsertOneModel<>(statsDoc));
                totalDocuments++;
                
                // Batch insert when we reach batch size
                if (writeModels.size() >= BATCH_SIZE) {
                    insertBatch(writeModels);
                    writeModels.clear();
                }
                
                if (totalDocuments % 10000 == 0) {
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
        
        logger.info("Starting balance phase for namespace: {}", namespace);
        
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
        
        // Group by chunkId and count
        pipeline.add(group("$chunkId", sum("count", 1)));
        
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
                String chunkId = result.getString("_id");
                int count = result.getInteger("count");
                
                if (isDryRun) {
                    logger.info("Chunk: {}, Count: {}", chunkId, count);
                    processedCount++;
                    continue;
                }
                
                // Get destination shard
                int destShardIndex = destShardIndexList.get(roundRobinIndex % destShardIndexList.size());
                String destShardId = shardIds.get(destShardIndex);
                
                logger.info("Moving chunk {} (count: {}) to shard {} (index {})", chunkId, count, destShardId, destShardIndex);
                
                // Find the chunk in our chunk map to get the bounds
                NavigableMap<BsonValueWrapper, CountingMegachunk> nsChunkMap = chunkMap.get(namespace);
                CountingMegachunk chunk = null;
                for (CountingMegachunk c : nsChunkMap.values()) {
                    if (chunkId.equals(c.getId())) {
                        chunk = c;
                        break;
                    }
                }
                
                if (chunk == null) {
                    logger.error("Could not find chunk {} in chunk map", chunkId);
                    continue;
                }
                
                // Perform moveChunk operation with retry logic
                boolean success = moveChunkWithRetry(namespace, chunk, destShardId, 10);
                
                if (success) {
                    logger.info("Successfully moved chunk {} to shard {}", chunkId, destShardId);
                    
                    // Update stats collection with move:true for this chunkId
                    try {
                        statsCollection.updateMany(
                            eq("chunkId", chunkId),
                            set("move", true)
                        );
                        logger.debug("Updated stats collection for chunk {}", chunkId);
                    } catch (Exception e) {
                        logger.warn("Failed to update stats collection for chunk {}: {}", chunkId, e.getMessage());
                    }
                } else {
                    logger.error("Failed to move chunk {} to shard {}", chunkId, destShardId);
                }
                
                roundRobinIndex++;
                processedCount++;
            }
        }
        
        if (isDryRun) {
            logger.info("Dry run completed. Showed {} chunks that would be moved", processedCount);
        } else {
            logger.info("Balance phase completed. Processed {} chunks", processedCount);
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