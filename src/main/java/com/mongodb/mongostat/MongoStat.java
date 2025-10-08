package com.mongodb.mongostat;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoIterable;
import com.mongodb.shardsync.ShardClient;

public class MongoStat {
    
    private static Logger logger = LoggerFactory.getLogger(MongoStat.class);
    
    private List<MongoClient> mongoClients = new ArrayList<MongoClient>();
    private Map<String, MongoClient> shardClients = new HashMap<>();
    private List<ServerStatus> serverStatuses = new ArrayList<ServerStatus>();
    private Map<String, WiredTigerCacheStats> wtCacheStats = new HashMap<>();
    private Map<String, Map<String, CollectionStats>> collectionStats = new HashMap<>();
    
    private String[] uris;
    private MongoStatConfiguration config = new MongoStatConfiguration();
    
    private ExecutorService executor;
    private ObjectMapper objectMapper = new ObjectMapper();
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private int lineCount = 0;
    
    private ShardClient shardClient;

    public void setUris(String[] uris) {
        this.uris = uris;
    }
    
    public void setConfiguration(MongoStatConfiguration config) {
        this.config = config;
    }
    
    public void setJsonOutput(boolean jsonOutput) {
        this.config = this.config.jsonOutput(jsonOutput);
    }
    
    public void setIncludeWiredTigerStats(boolean includeWiredTigerStats) {
        this.config = this.config.includeWiredTigerStats(includeWiredTigerStats);
    }
    
    public void setIncludeCollectionStats(boolean includeCollectionStats) {
        this.config = this.config.includeCollectionStats(includeCollectionStats);
    }
    
    public void setDetailedOutput(boolean detailedOutput) {
        this.config = this.config.detailedOutput(detailedOutput);
    }
    
    // Factory method for external usage
    public static MongoStat create(String uri, MongoStatConfiguration config) {
        MongoStat mongoStat = new MongoStat();
        mongoStat.setUris(new String[]{uri});
        mongoStat.setConfiguration(config);
        return mongoStat;
    }

    public void init() {
        // Use ShardClient for better shard management
        if (uris.length == 1) {
            String uri = uris[0];
            
            // Check if this is a mongos
            ConnectionString connectionString = new ConnectionString(uri);
            MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .build();
            MongoClient tempClient = MongoClients.create(mongoClientSettings);
            
            boolean isMongos = false;
            try {
                Document isDbGridResponse = tempClient.getDatabase("admin").runCommand(new Document("isdbgrid", 1));
                Object isDbGrid = isDbGridResponse.get("isdbgrid");
                if (isDbGrid != null) {
                    isMongos = true;
                }
            } catch (MongoCommandException mce) {
            }
            
            if (isMongos) {
                shardClient = new ShardClient("mongostat", uri);
                shardClient.init();
                shardClient.populateShardMongoClients();
                
                shardClients = shardClient.getShardMongoClients();
                for (Map.Entry<String, MongoClient> entry : shardClients.entrySet()) {
                    mongoClients.add(entry.getValue());
                    ServerStatus status = new ServerStatus();
                    serverStatuses.add(status);
                    
                    if (config.isIncludeWiredTigerStats()) {
                        wtCacheStats.put(entry.getKey(), new WiredTigerCacheStats(entry.getKey()));
                    }
                    
                    if (config.isIncludeCollectionStats()) {
                        collectionStats.put(entry.getKey(), new HashMap<>());
                    }
                }
            } else {
                mongoClients.add(tempClient);
                ServerStatus status = new ServerStatus();
                serverStatuses.add(status);
                
                String shardName = "standalone";
                shardClients.put(shardName, tempClient);
                
                if (config.isIncludeWiredTigerStats()) {
                    wtCacheStats.put(shardName, new WiredTigerCacheStats(shardName));
                }
                
                if (config.isIncludeCollectionStats()) {
                    collectionStats.put(shardName, new HashMap<>());
                }
            }
        } else {
            // Legacy multi-URI support
            for (String uri : uris) {
                logger.debug("Connecting to " + uri);
                ConnectionString connectionString = new ConnectionString(uri);
                MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                        .applyConnectionString(connectionString)
                        .build();
                 
                MongoClient client = MongoClients.create(mongoClientSettings);
                mongoClients.add(client);
                
                ServerStatus status = new ServerStatus();
                serverStatuses.add(status);
            }
        }
        
        // Initialize thread pool for parallel stats collection
        executor = Executors.newFixedThreadPool(Math.max(1, shardClients.size() * 2));
    }

    public void run() {
        while (true) {
            runSnapshot();
            sleep(config.getIntervalMs());
        }
    }
    
    public void runSnapshot() {
        if (config.isJsonOutput()) {
            runJsonOutput();
        } else {
            runTableOutput();
        }
    }
    
    private void runTableOutput() {
        int index = 0;
        
        if (config.isIncludeWiredTigerStats() || config.isIncludeCollectionStats()) {
            if (config.isDetailedOutput()) {
                printDetailedHeader();
            } else {
                printEnhancedHeader();
            }
        } else {
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %13s%13s%13s%13s %n",
                    LocalTime.now().format(timeFormatter),
                    "replicaSet", "insert", "query", "update", "delete", "totInserts", "totQueries", "totUpdates", "totDeletes");
        }
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        
        for (MongoClient client : mongoClients) {
            final int currentIndex = index;
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                Document serverStatus = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));
                
                ServerStatus status = null;
                if (serverStatuses.size() >= currentIndex + 1) {
                    status = serverStatuses.get(currentIndex);
                } else {
                    status = new ServerStatus();
                    serverStatuses.add(currentIndex, status);
                }
                status.updateServerStatus(serverStatus);
                
                String shardName = getShardNameForClient(client);
                
                // Update WT cache stats if enabled
                if (config.isIncludeWiredTigerStats() && shardName != null) {
                    WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
                    if (wtStats != null) {
                        wtStats.updateFromServerStatus(serverStatus);
                    }
                }
                
                // Update collection stats if enabled
                if (config.isIncludeCollectionStats() && shardName != null) {
                    updateCollectionStatsForShard(client, shardName);
                }
                
                if (config.isIncludeWiredTigerStats() || config.isIncludeCollectionStats()) {
                    if (config.isDetailedOutput() && config.isIncludeCollectionStats()) {
                        printDetailedCollectionReport(status, shardName);
                    } else {
                        printEnhancedReport(status, shardName);
                    }
                } else {
                    status.report();
                }
            }, executor);
            
            futures.add(future);
            index++;
        }
        
        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }
    
    private String getShardNameForClient(MongoClient client) {
        for (Map.Entry<String, MongoClient> entry : shardClients.entrySet()) {
            if (entry.getValue() == client) {
                return entry.getKey();
            }
        }
        return null;
    }
    
    private void updateCollectionStatsForShard(MongoClient client, String shardName) {
        try {
            MongoIterable<String> databaseNames = client.listDatabaseNames();
            Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);

            // Get server max cache bytes from WiredTiger stats
            WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
            Long serverMaxCacheBytes = wtStats != null ? wtStats.getMaxCacheBytes() : null;

            logger.debug("Starting collection stats update for shard {}, serverMaxCacheBytes: {}", shardName, serverMaxCacheBytes);
            int dbCount = 0;
            int collCount = 0;
            int successCount = 0;
            int failCount = 0;

            for (String dbName : databaseNames) {
                if ("admin".equals(dbName) || "config".equals(dbName) || "local".equals(dbName)) {
                    continue;
                }
                dbCount++;

                MongoIterable<String> collectionNames = client.getDatabase(dbName).listCollectionNames();
                for (String collName : collectionNames) {
                    String namespace = dbName + "." + collName;
                    collCount++;

                    CollectionStats collStats = shardCollStats.computeIfAbsent(namespace,
                            k -> new CollectionStats(namespace, shardName));

                    // Set server max cache bytes for dirty% calculation
                    collStats.setServerMaxCacheBytes(serverMaxCacheBytes);

                    try {
                        // Call collStats - indexDetails are included in response by default if supported
                        // The indexDetails parameter is NOT a command parameter, it's just used by the shell
                        // to filter the response. We always get the full response.
                        Document collStatsCommand = new Document("collStats", collName);
                        Document collStatsDoc = client.getDatabase(dbName)
                                .runCommand(collStatsCommand);

                        // Debug: check if indexDetails are in the response for first collection only
                        if (collCount == 1) {
                            logger.warn("Sample collStats response for {}: has indexDetails={}, has indexSizes={}",
                                    namespace,
                                    collStatsDoc.containsKey("indexDetails"),
                                    collStatsDoc.containsKey("indexSizes"));
                            if (collStatsDoc.containsKey("indexDetails")) {
                                Document indexDetails = (Document) collStatsDoc.get("indexDetails");
                                logger.warn("indexDetails has {} indexes", indexDetails.keySet().size());
                                // Log first index details
                                String firstIndexName = indexDetails.keySet().iterator().next();
                                Document firstIndex = (Document) indexDetails.get(firstIndexName);
                                logger.warn("First index '{}' has cache section: {}",
                                        firstIndexName, firstIndex.containsKey("cache"));
                                if (firstIndex.containsKey("cache")) {
                                    Document cache = (Document) firstIndex.get("cache");
                                    logger.warn("Cache keys: {}", cache.keySet());
                                }
                            }
                        }

                        collStats.updateFromCollStats(collStatsDoc);
                        successCount++;
                    } catch (Exception e) {
                        logger.warn("Error getting collStats for {} on shard {}: {}", namespace, shardName, e.getMessage());
                        failCount++;
                    }
                }
            }
            logger.debug("Collection stats update complete for shard {}: {} databases, {} collections, {} successful, {} failed",
                    shardName, dbCount, collCount, successCount, failCount);
            logger.debug("Collection stats map for shard {} now contains {} entries", shardName, shardCollStats.size());
        } catch (Exception e) {
            logger.error("Error updating collection stats for shard " + shardName, e);
        }
    }
    
    private int maxShardWidth = 20;
    private int maxCollectionWidth = 80;

    private void calculateColumnWidths() {
        int minShardWidth = 20;
        int minCollectionWidth = 80;

        maxShardWidth = minShardWidth;
        maxCollectionWidth = minCollectionWidth;

        // Find max shard name width
        for (String shardName : shardClients.keySet()) {
            if (shardName != null && shardName.length() > maxShardWidth) {
                maxShardWidth = shardName.length();
            }
        }

        // Find max collection name width
        for (Map<String, CollectionStats> shardStats : collectionStats.values()) {
            if (shardStats != null) {
                for (String namespace : shardStats.keySet()) {
                    if (namespace != null && namespace.length() > maxCollectionWidth) {
                        maxCollectionWidth = namespace.length();
                    }
                }
            }
        }
    }

    private void printDetailedHeader() {
        calculateColumnWidths();
        System.out.printf("%-8s %-" + maxShardWidth + "s %-" + maxCollectionWidth + "s %6s %6s %6s %6s %8s %8s %8s %8s %8s %8s %7s %7s%n",
                "Time", "Shard", "Collection", "ins", "qry", "upd", "del", "dataGB", "idxGB", "cacheMB", "dirtyMB", "readMB", "writMB", "dirty%", "idxDty%");
        lineCount = 0;
    }
    
    private void printEnhancedHeader() {
        if (config.isIncludeWiredTigerStats() && config.isIncludeCollectionStats()) {
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10s%10s%8s%8s %10s%10s%10s %n",
                    "Time    ", "shard", "ins", "qry", "upd", "del", "cache(MB)", "dirty%", "pgRd", "pgWr", "colls", "dataGB", "idxGB");
        } else if (config.isIncludeWiredTigerStats()) {
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10s%10s%8s%8s %n",
                    "Time    ", "shard", "ins", "qry", "upd", "del", "cache(MB)", "dirty%", "pgRd", "pgWr");
        } else if (config.isIncludeCollectionStats()) {
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10s%10s%10s %n",
                    "Time    ", "shard", "ins", "qry", "upd", "del", "colls", "dataGB", "idxGB");
        }
    }
    
    private void printEnhancedReport(ServerStatus status, String shardName) {
        WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
        Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);
        
        if (config.isIncludeWiredTigerStats() && config.isIncludeCollectionStats()) {
            long totalDataSize = 0;
            long totalIndexSize = 0;
            int collCount = 0;
            
            if (shardCollStats != null) {
                for (CollectionStats cs : shardCollStats.values()) {
                    if (cs.getDataSize() != null) totalDataSize += cs.getDataSize();
                    if (cs.getIndexSize() != null) totalIndexSize += cs.getIndexSize();
                    collCount++;
                }
            }
            
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10.1f%9.1f%%%8d%8d %10d%10.2f%10.2f %n",
                    LocalTime.now().format(timeFormatter),
                    shardName != null ? shardName : "unknown",
                    status.getCurrentInserts(), status.getCurrentQueries(), status.getCurrentUpdates(), status.getCurrentDeletes(),
                    wtStats != null ? wtStats.getCurrentCacheBytes() / 1024.0 / 1024.0 : 0.0,
                    wtStats != null ? wtStats.getDirtyFillRatio() * 100 : 0.0,
                    wtStats != null ? wtStats.getDelta("pagesReadIntoCache") : 0L,
                    wtStats != null ? wtStats.getDelta("pagesWrittenFromCache") : 0L,
                    collCount,
                    totalDataSize / 1024.0 / 1024.0 / 1024.0,
                    totalIndexSize / 1024.0 / 1024.0 / 1024.0);
        } else if (config.isIncludeWiredTigerStats()) {
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10.1f%9.1f%%%8d%8d %n",
                    LocalTime.now().format(timeFormatter),
                    shardName != null ? shardName : "unknown",
                    status.getCurrentInserts(), status.getCurrentQueries(), status.getCurrentUpdates(), status.getCurrentDeletes(),
                    wtStats != null ? wtStats.getCurrentCacheBytes() / 1024.0 / 1024.0 : 0.0,
                    wtStats != null ? wtStats.getDirtyFillRatio() * 100 : 0.0,
                    wtStats != null ? wtStats.getDelta("pagesReadIntoCache") : 0L,
                    wtStats != null ? wtStats.getDelta("pagesWrittenFromCache") : 0L);
        } else if (config.isIncludeCollectionStats()) {
            long totalDataSize = 0;
            long totalIndexSize = 0;
            int collCount = 0;
            
            if (shardCollStats != null) {
                for (CollectionStats cs : shardCollStats.values()) {
                    if (cs.getDataSize() != null) totalDataSize += cs.getDataSize();
                    if (cs.getIndexSize() != null) totalIndexSize += cs.getIndexSize();
                    collCount++;
                }
            }
            
            System.out.printf(
                    "%s %-15s%8s%8s%8s%8s %10d%10.2f%10.2f %n",
                    LocalTime.now().format(timeFormatter),
                    shardName != null ? shardName : "unknown",
                    status.getCurrentInserts(), status.getCurrentQueries(), status.getCurrentUpdates(), status.getCurrentDeletes(),
                    collCount,
                    totalDataSize / 1024.0 / 1024.0 / 1024.0,
                    totalIndexSize / 1024.0 / 1024.0 / 1024.0);
        }
    }
    
    private void printDetailedCollectionReport(ServerStatus status, String shardName) {
        Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);
        WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
        String timestamp = LocalTime.now().format(timeFormatter);
        String shard = shardName != null ? shardName : "unknown";

        logger.debug("printDetailedCollectionReport called for shard {}, collStats map size: {}",
                shard, shardCollStats != null ? shardCollStats.size() : "null");

        // Check if we need to print header again
        if (lineCount >= 25) {
            System.out.println();
            printDetailedHeader();
        }

        // Print shard summary line
        String format = "%-8s %-" + maxShardWidth + "s %-" + maxCollectionWidth + "s %6s %6s %6s %6s %8.1f %8.1f %8.1f %8.1f %8.1f %8.1f %6.1f%% %6.1f%%%n";
        System.out.printf(format,
                timestamp, shard, "[SHARD TOTAL]",
                status.getCurrentInserts(), status.getCurrentQueries(),
                status.getCurrentUpdates(), status.getCurrentDeletes(),
                0.0, 0.0,
                wtStats != null ? wtStats.getCurrentCacheBytes() / 1024.0 / 1024.0 : 0.0,
                wtStats != null ? wtStats.getDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                0.0, 0.0,
                wtStats != null ? wtStats.getDirtyFillRatio() * 100 : 0.0,
                0.0);  // No index dirty% for shard total line
        lineCount++;

        // Print collection details if available
        if (shardCollStats != null && !shardCollStats.isEmpty()) {
            logger.debug("Processing {} collections for printing", shardCollStats.size());
            String collFormat = "%-8s %-" + maxShardWidth + "s %-" + maxCollectionWidth + "s %6s %6s %6s %6s %8.2f %8.2f %8.1f %8.1f %8.1f %8.1f %6.1f%% %6.1f%%%n";

            // Sort collections by cacheMB descending
            shardCollStats.values().stream()
                .sorted((cs1, cs2) -> {
                    double cache1 = cs1.getCacheCurrentBytes() != null ? cs1.getCacheCurrentBytes() : 0L;
                    double cache2 = cs2.getCacheCurrentBytes() != null ? cs2.getCacheCurrentBytes() : 0L;
                    return Double.compare(cache2, cache1);
                })
                .forEach(cs -> {
                    // Calculate display value
                    double cacheMB = cs.getCacheCurrentBytes() != null ? cs.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;

                    // Skip collections where display value would be 0.0 (less than 0.05 MB)
                    if (cacheMB < 0.05) {
                        return;
                    }

                    System.out.printf(collFormat,
                            timestamp, shard, cs.getNamespace(),
                            "-", "-", "-", "-",
                            cs.getDataSize() != null ? cs.getDataSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                            cs.getIndexSize() != null ? cs.getIndexSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                            cacheMB,
                            cs.getCacheDirtyBytes() != null ? cs.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                            cs.getDelta("cacheBytesRead") / 1024.0 / 1024.0,
                            cs.getDelta("cacheBytesWritten") / 1024.0 / 1024.0,
                            cs.getDirtyFillRatio() * 100,
                            cs.getIndexDirtyFillRatio() * 100);
                    lineCount++;

                    // If includeIndexDetails is true, print each index as a separate row
                    if (config.isIncludeIndexDetails() && cs.getIndexStats() != null && !cs.getIndexStats().isEmpty()) {
                        cs.getIndexStats().values().stream()
                            .sorted((idx1, idx2) -> {
                                double idxCache1 = idx1.getCacheCurrentBytes() != null ? idx1.getCacheCurrentBytes() : 0L;
                                double idxCache2 = idx2.getCacheCurrentBytes() != null ? idx2.getCacheCurrentBytes() : 0L;
                                return Double.compare(idxCache2, idxCache1);
                            })
                            .forEach(idx -> {
                                double idxCacheMB = idx.getCacheCurrentBytes() != null ? idx.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;
                                if (idxCacheMB < 0.05) {
                                    return;
                                }
                                System.out.printf(collFormat,
                                        timestamp, shard, "  [idx] " + idx.getIndexName(),
                                        "-", "-", "-", "-",
                                        0.0,
                                        idx.getIndexSize() != null ? idx.getIndexSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                                        idxCacheMB,
                                        idx.getCacheDirtyBytes() != null ? idx.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                                        idx.getDelta("cacheBytesRead") / 1024.0 / 1024.0,
                                        idx.getDelta("cacheBytesWritten") / 1024.0 / 1024.0,
                                        idx.getDirtyFillRatio() * 100,
                                        0.0);  // No index dirty% for individual indexes
                                lineCount++;
                            });
                    }
                });
        } else {
            logger.warn("No collection stats available for shard {} - map is {}", shard, shardCollStats != null ? "empty" : "null");
        }
    }
    
    private void runJsonOutput() {
        ObjectNode rootNode = objectMapper.createObjectNode();
        rootNode.put("timestamp", System.currentTimeMillis());
        rootNode.put("time", LocalTime.now().format(timeFormatter));
        
        ArrayNode shardsArray = objectMapper.createArrayNode();
        
        List<CompletableFuture<ObjectNode>> futures = new ArrayList<>();
        
        int index = 0;
        for (MongoClient client : mongoClients) {
            final int currentIndex = index;
            
            CompletableFuture<ObjectNode> future = CompletableFuture.supplyAsync(() -> {
                ObjectNode shardNode = objectMapper.createObjectNode();
                
                try {
                    Document serverStatus = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));
                    
                    ServerStatus status = null;
                    if (serverStatuses.size() >= currentIndex + 1) {
                        status = serverStatuses.get(currentIndex);
                    } else {
                        status = new ServerStatus();
                        serverStatuses.add(currentIndex, status);
                    }
                    status.updateServerStatus(serverStatus);
                    
                    String shardName = getShardNameForClient(client);
                    shardNode.put("shard", shardName != null ? shardName : "unknown");
                    
                    // Basic server stats
                    addServerStatsToJson(shardNode, status);
                    
                    // WT cache stats if enabled
                    if (config.isIncludeWiredTigerStats() && shardName != null) {
                        WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
                        if (wtStats != null) {
                            wtStats.updateFromServerStatus(serverStatus);
                            addWtCacheStatsToJson(shardNode, wtStats);
                        }
                    }
                    
                    // Collection stats if enabled
                    if (config.isIncludeCollectionStats() && shardName != null) {
                        updateCollectionStatsForShard(client, shardName);
                        addCollectionStatsToJson(shardNode, shardName);
                    }
                    
                } catch (Exception e) {
                    logger.error("Error collecting stats for shard", e);
                    shardNode.put("error", e.getMessage());
                }
                
                return shardNode;
            }, executor);
            
            futures.add(future);
            index++;
        }
        
        // Wait for all futures and collect results
        for (CompletableFuture<ObjectNode> future : futures) {
            try {
                shardsArray.add(future.get());
            } catch (Exception e) {
                logger.error("Error getting shard stats", e);
            }
        }
        
        rootNode.set("shards", shardsArray);
        
        try {
            System.out.println(objectMapper.writeValueAsString(rootNode));
        } catch (Exception e) {
            logger.error("Error serializing JSON output", e);
        }
    }
    
    private void addServerStatsToJson(ObjectNode shardNode, ServerStatus status) {
        ObjectNode opsNode = objectMapper.createObjectNode();
        opsNode.put("insert", status.getCurrentInserts());
        opsNode.put("query", status.getCurrentQueries());
        opsNode.put("update", status.getCurrentUpdates());
        opsNode.put("delete", status.getCurrentDeletes());
        shardNode.set("operations", opsNode);
        
        ObjectNode totalsNode = objectMapper.createObjectNode();
        totalsNode.put("insert", status.getTotalInserts());
        totalsNode.put("query", status.getTotalQueries());
        totalsNode.put("update", status.getTotalUpdates());
        totalsNode.put("delete", status.getTotalDeletes());
        shardNode.set("totals", totalsNode);
    }
    
    private void addWtCacheStatsToJson(ObjectNode shardNode, WiredTigerCacheStats wtStats) {
        ObjectNode cacheNode = objectMapper.createObjectNode();
        
        ObjectNode memoryNode = objectMapper.createObjectNode();
        memoryNode.put("currentBytes", wtStats.getCurrentCacheBytes());
        memoryNode.put("maxBytes", wtStats.getMaxCacheBytes());
        memoryNode.put("dirtyBytes", wtStats.getDirtyBytes());
        memoryNode.put("internalPagesBytes", wtStats.getInternalPagesBytes());
        memoryNode.put("pageImagesBytes", wtStats.getPageImagesBytes());
        memoryNode.put("nonPageImagesBytes", wtStats.getNonPageImagesBytes());
        memoryNode.put("utilization", wtStats.getCacheUtilization());
        memoryNode.put("dirtyFillRatio", wtStats.getDirtyFillRatio());
        cacheNode.set("memory", memoryNode);
        
        ObjectNode activityNode = objectMapper.createObjectNode();
        activityNode.put("pagesRead", wtStats.getDelta("pagesReadIntoCache"));
        activityNode.put("pagesWritten", wtStats.getDelta("pagesWrittenFromCache"));
        activityNode.put("pagesEvictedApp", wtStats.getDelta("pagesEvictedByAppThreads"));
        activityNode.put("pagesEvictedWorker", wtStats.getDelta("pagesEvictedByWorkerThreads"));
        cacheNode.set("activity", activityNode);
        
        ObjectNode evictionNode = objectMapper.createObjectNode();
        evictionNode.put("unableToEvict", wtStats.getDelta("pagesSelectedUnableToEvict"));
        evictionNode.put("exceededMax", wtStats.getDelta("pagesEvictedExceededMax"));
        evictionNode.put("deletedChains", wtStats.getDelta("pagesEvictedDeletedChains"));
        cacheNode.set("eviction", evictionNode);
        
        shardNode.set("wiredTigerCache", cacheNode);
    }
    
    private void addCollectionStatsToJson(ObjectNode shardNode, String shardName) {
        Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);
        if (shardCollStats == null || shardCollStats.isEmpty()) {
            return;
        }
        
        ArrayNode collectionsArray = objectMapper.createArrayNode();
        
        for (CollectionStats cs : shardCollStats.values()) {
            ObjectNode collNode = objectMapper.createObjectNode();
            collNode.put("namespace", cs.getNamespace());
            collNode.put("dataSize", cs.getDataSize());
            collNode.put("indexSize", cs.getIndexSize());
            collNode.put("totalSize", cs.getTotalSize());
            collNode.put("documentCount", cs.getDocumentCount());
            collNode.put("dirtyFillRatio", cs.getDirtyFillRatio());
            
            ObjectNode cacheNode = objectMapper.createObjectNode();
            cacheNode.put("currentBytes", cs.getCacheCurrentBytes());
            cacheNode.put("dirtyBytes", cs.getCacheDirtyBytes());
            cacheNode.put("pagesRead", cs.getDelta("cachePagesRead"));
            cacheNode.put("pagesWritten", cs.getDelta("cachePagesWritten"));
            collNode.set("cache", cacheNode);
            
            collectionsArray.add(collNode);
        }
        
        shardNode.set("collections", collectionsArray);
    }
    
    private void sleep(long sleep) {
        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
        }
    }

}
