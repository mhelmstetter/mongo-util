package com.mongodb.mongostat;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.shardsync.ShardClient;

public class MongoStat {
    
    private static Logger logger = LoggerFactory.getLogger(MongoStat.class);
    
    private List<MongoClient> mongoClients = new ArrayList<MongoClient>();
    private Map<String, MongoClient> shardClients = new HashMap<>();
    private List<ServerStatus> serverStatuses = new ArrayList<ServerStatus>();
    private Map<String, WiredTigerCacheStats> wtCacheStats = new HashMap<>();
    private Map<String, Map<String, CollectionStats>> collectionStats = new HashMap<>();
    private Map<String, Long> shardUptimeSeconds = new ConcurrentHashMap<>();
    
    private String[] uris;
    private List<String> explicitShardNames = null;
    private boolean isMongos = false;
    private MongoClient catalogClient;
    private MongoStatConfiguration config = new MongoStatConfiguration();
    
    private ExecutorService executor;
    private ObjectMapper objectMapper = new ObjectMapper();
    private DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");
    private int lineCount = 0;
    private int snapshotCount = 0;

    private ShardClient shardClient;

    public void setUris(String[] uris) {
        this.uris = uris;
    }

    public void setExplicitShardNames(List<String> shardNames) {
        this.explicitShardNames = shardNames;
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
            
            try {
                Document isDbGridResponse = tempClient.getDatabase("admin").runCommand(new Document("isdbgrid", 1));
                Object isDbGrid = isDbGridResponse.get("isdbgrid");
                if (isDbGrid != null) {
                    isMongos = true;
                }
            } catch (MongoCommandException mce) {
            }

            if (isMongos && explicitShardNames != null && !explicitShardNames.isEmpty()) {
                catalogClient = tempClient;
                Map<String, String> resolvedUris = resolveShardUris(tempClient, uri, explicitShardNames);
                for (Map.Entry<String, String> entry : resolvedUris.entrySet()) {
                    String shardName = entry.getKey();
                    String shardUri = entry.getValue();
                    MongoClient shardMongoClient = MongoClients.create(MongoClientSettings.builder()
                            .applyConnectionString(new ConnectionString(shardUri))
                            .build());
                    mongoClients.add(shardMongoClient);
                    shardClients.put(shardName, shardMongoClient);
                    serverStatuses.add(new ServerStatus());
                    if (config.isIncludeWiredTigerStats()) {
                        wtCacheStats.put(shardName, new WiredTigerCacheStats(shardName));
                    }
                    if (config.isIncludeCollectionStats()) {
                        collectionStats.put(shardName, new HashMap<>());
                    }
                }
            } else if (isMongos) {
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
                    logger.warn("Collection stats require a mongos connection. Connect via mongos for per-collection statistics. Disabling collection stats.");
                    config.includeCollectionStats(false);
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
        // Skip first snapshot for delta metrics - need previous values to calculate deltas
        if (snapshotCount == 0) {
            logger.info("Collecting initial snapshot, please wait {} seconds for first output...", config.getIntervalMs() / 1000);
            snapshotCount++;

            // Still need to collect the data, just don't output it
            if (config.isJsonOutput()) {
                runJsonOutputInternal(false); // false = skip output
            } else {
                runTableOutputInternal(false); // false = skip output
            }
            return;
        }

        snapshotCount++;

        if (config.isJsonOutput()) {
            runJsonOutputInternal(true);
        } else {
            runTableOutputInternal(true);
        }
    }
    
    private void runTableOutputInternal(boolean shouldOutput) {
        int index = 0;

        // Skip printing header when in pivot mode or when shouldOutput is false
        if (shouldOutput && !config.isShardPivot()) {
            if (config.isIncludeWiredTigerStats() || config.isIncludeCollectionStats()) {
                if (config.isDetailedOutput() && config.isIncludeCollectionStats()) {
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
        }
        
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        final boolean finalShouldOutput = shouldOutput; // Make effectively final for lambda

        for (MongoClient client : mongoClients) {
            final int currentIndex = index;

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                String shardName = getShardNameForClient(client);
                ServerStatus status = null;
                Document serverStatus = null;

                // Try to get serverStatus, but handle authorization errors gracefully
                if (config.getManualCacheSizeBytes() == null) {
                    try {
                        serverStatus = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));

                        if (serverStatuses.size() >= currentIndex + 1) {
                            status = serverStatuses.get(currentIndex);
                        } else {
                            status = new ServerStatus();
                            serverStatuses.add(currentIndex, status);
                        }
                        status.updateServerStatus(serverStatus);

                        // Store uptime for cumulative normalization
                        if (shardName != null) {
                            Number uptime = (Number) serverStatus.get("uptime");
                            if (uptime != null) {
                                shardUptimeSeconds.put(shardName, uptime.longValue());
                            }
                        }

                        // Update WT cache stats if enabled
                        if (config.isIncludeWiredTigerStats() && shardName != null) {
                            WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
                            if (wtStats != null) {
                                wtStats.updateFromServerStatus(serverStatus);
                            }
                        }
                    } catch (com.mongodb.MongoCommandException e) {
                        if (e.getErrorCode() == 13) { // Unauthorized
                            logger.warn("Not authorized to run serverStatus on {}. Use --cache-size-gb to specify cache size manually.", shardName);
                            // Create empty status for operation counters
                            if (serverStatuses.size() >= currentIndex + 1) {
                                status = serverStatuses.get(currentIndex);
                            } else {
                                status = new ServerStatus();
                                serverStatuses.add(currentIndex, status);
                            }
                        } else {
                            throw e;
                        }
                    }
                } else {
                    // Using manual cache size, skip serverStatus
                    logger.debug("Using manual cache size, skipping serverStatus for {}", shardName);
                    if (serverStatuses.size() >= currentIndex + 1) {
                        status = serverStatuses.get(currentIndex);
                    } else {
                        status = new ServerStatus();
                        serverStatuses.add(currentIndex, status);
                    }

                    // Set manual cache size on WiredTiger stats
                    if (config.isIncludeWiredTigerStats() && shardName != null) {
                        WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
                        if (wtStats != null) {
                            wtStats.setManualMaxCacheBytes(config.getManualCacheSizeBytes());
                        }
                    }
                }
                
                // Update collection stats if enabled
                if (config.isIncludeCollectionStats() && shardName != null) {
                    updateCollectionStatsForShard(client, shardName);
                }

                // Skip per-shard printing if in pivot mode or if shouldOutput is false
                if (finalShouldOutput && !config.isShardPivot()) {
                    if (config.isIncludeWiredTigerStats() || config.isIncludeCollectionStats()) {
                        if (config.isDetailedOutput() && config.isIncludeCollectionStats()) {
                            synchronized(this) {
                                printDetailedCollectionReport(status, shardName);
                            }
                        } else {
                            synchronized(this) {
                                printEnhancedReport(status, shardName);
                            }
                        }
                    } else {
                        status.report();
                    }
                }
            }, executor);
            
            futures.add(future);
            index++;
        }
        
        // Wait for all futures to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // After all shards are processed, print pivot report if enabled
        if (shouldOutput && config.isShardPivot() && config.isIncludeCollectionStats()) {
            printShardPivotReport();
        }
    }
    
    public static void listShards(String mongosUri) {
        try (MongoClient client = MongoClients.create(mongosUri)) {
            Document result = client.getDatabase("admin").runCommand(new Document("listShards", 1));
            List<Document> shards = result.getList("shards", Document.class);
            System.out.printf("%-30s %s%n", "Shard", "Hosts");
            for (Document shard : shards) {
                System.out.printf("%-30s %s%n", shard.getString("_id"), shard.getString("host"));
            }
        }
    }

    private Map<String, String> resolveShardUris(MongoClient mongosClient, String mainUri, List<String> shardNames) {
        Document result = mongosClient.getDatabase("admin").runCommand(new Document("listShards", 1));
        List<Document> shards = result.getList("shards", Document.class);
        Map<String, String> resolved = new java.util.LinkedHashMap<>();
        for (String name : shardNames) {
            Document match = shards.stream()
                    .filter(s -> name.equals(s.getString("_id")))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Shard not found: " + name
                            + ". Use --shard with no args to list available shards."));
            resolved.put(name, buildShardUri(mainUri, match.getString("host")));
        }
        return resolved;
    }

    private String buildShardUri(String mainUri, String shardHost) {
        // shardHost format from listShards: "rsName/host1:port,host2:port,..."
        ConnectionString cs = new ConnectionString(mainUri);
        int slash = shardHost.indexOf('/');
        String rsName = slash >= 0 ? shardHost.substring(0, slash) : null;
        String hosts = slash >= 0 ? shardHost.substring(slash + 1) : shardHost;

        StringBuilder sb = new StringBuilder("mongodb://");
        if (cs.getUsername() != null) {
            try {
                sb.append(java.net.URLEncoder.encode(cs.getUsername(), "UTF-8"));
                if (cs.getPassword() != null) {
                    sb.append(":").append(java.net.URLEncoder.encode(new String(cs.getPassword()), "UTF-8"));
                }
                sb.append("@");
            } catch (java.io.UnsupportedEncodingException e) {
                throw new IllegalStateException("UTF-8 not supported", e);
            }
        }
        sb.append(hosts).append("/?");
        if (rsName != null) sb.append("replicaSet=").append(rsName).append("&");
        if (Boolean.TRUE.equals(cs.getSslEnabled())) sb.append("tls=true&");
        String authSource = cs.getCredential() != null ? cs.getCredential().getSource() : null;
        if (authSource != null && !authSource.isEmpty()) sb.append("authSource=").append(authSource).append("&");
        // trim trailing & or ?
        String result = sb.toString().replaceAll("[&?]$", "");
        logger.debug("Resolved shard URI: {}", result.replaceAll(":([^@/]+)@", ":***@"));
        return result;
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
        if (config.isIncludeIndexDetails() || catalogClient != null) {
            updateCollectionStatsForShardViaCollStats(client, shardName);
        } else {
            updateCollectionStatsForShardViaInternalAgg(client, shardName);
        }
    }

    @SuppressWarnings("unchecked")
    private void updateCollectionStatsForShardViaInternalAgg(MongoClient client, String shardName) {
        try {
            Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);
            WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
            Long serverMaxCacheBytes = wtStats != null ? wtStats.getMaxCacheBytes() : null;

            Document statsOptions = new Document("stats",
                new Document("storageStats",
                    new Document("verbose", false)
                        .append("waitForLock", true)
                        .append("numericOnly", false)));

            List<Document> pipeline = new ArrayList<>();
            pipeline.add(new Document("$_internalAllCollectionStats", statsOptions));

            Document command = new Document("aggregate", 1)
                .append("pipeline", pipeline)
                .append("cursor", new Document());

            logger.debug("Running $_internalAllCollectionStats for shard {}", shardName);
            MongoDatabase adminDb = client.getDatabase("admin");
            Document response = adminDb.runCommand(command);

            Document cursor = response.get("cursor", Document.class);
            if (cursor == null) {
                throw new IllegalStateException("$_internalAllCollectionStats returned no cursor for shard " + shardName);
            }

            List<Document> firstBatch = (List<Document>) cursor.get("firstBatch");
            if (firstBatch != null) {
                processInternalCollStatsResults(firstBatch, shardCollStats, serverMaxCacheBytes, shardName);
            }

            Long cursorId = cursor.getLong("id");
            while (cursorId != null && cursorId != 0) {
                Document getMore = new Document("getMore", cursorId)
                    .append("collection", "$cmd.aggregate");
                Document moreResponse = adminDb.runCommand(getMore);
                Document moreCursor = moreResponse.get("cursor", Document.class);
                if (moreCursor == null) break;
                List<Document> nextBatch = (List<Document>) moreCursor.get("nextBatch");
                if (nextBatch != null && !nextBatch.isEmpty()) {
                    processInternalCollStatsResults(nextBatch, shardCollStats, serverMaxCacheBytes, shardName);
                }
                cursorId = moreCursor.getLong("id");
            }

            logger.debug("$_internalAllCollectionStats complete for shard {}: {} collections tracked",
                shardName, shardCollStats.size());
        } catch (Exception e) {
            throw new IllegalStateException("Failed to collect collection stats via $_internalAllCollectionStats for shard " + shardName, e);
        }
    }

    private void processInternalCollStatsResults(List<Document> docs, Map<String, CollectionStats> shardCollStats,
            Long serverMaxCacheBytes, String shardName) {
        for (Document doc : docs) {
            String ns = doc.getString("ns");
            if (ns == null) continue;

            int dot = ns.indexOf('.');
            if (dot <= 0) continue;

            String dbName = ns.substring(0, dot);
            String collName = ns.substring(dot + 1);

            if ("admin".equals(dbName) || "config".equals(dbName) || "local".equals(dbName)) continue;
            if (collName.startsWith("system.")) continue;

            CollectionStats collStats = shardCollStats.computeIfAbsent(ns,
                k -> new CollectionStats(ns, shardName));
            collStats.setServerMaxCacheBytes(serverMaxCacheBytes);
            collStats.updateFromInternalCollectionStats(doc);
        }
    }

    private void updateCollectionStatsForShardViaCollStats(MongoClient client, String shardName) {
        try {
            MongoClient listingClient = catalogClient != null ? catalogClient : client;
            MongoIterable<String> databaseNames = listingClient.listDatabaseNames();
            Map<String, CollectionStats> shardCollStats = collectionStats.get(shardName);

            WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
            Long serverMaxCacheBytes = wtStats != null ? wtStats.getMaxCacheBytes() : null;

            logger.debug("Starting collStats update for shard {}, serverMaxCacheBytes: {}", shardName, serverMaxCacheBytes);
            int dbCount = 0;
            int collCount = 0;
            int successCount = 0;
            int failCount = 0;

            for (String dbName : databaseNames) {
                if ("admin".equals(dbName) || "config".equals(dbName) || "local".equals(dbName)) {
                    continue;
                }
                dbCount++;

                MongoIterable<String> collectionNames = listingClient.getDatabase(dbName).listCollectionNames();
                for (String collName : collectionNames) {
                    if (collName.startsWith("system.")) {
                        continue;
                    }
                    String namespace = dbName + "." + collName;
                    collCount++;

                    CollectionStats collStats = shardCollStats.computeIfAbsent(namespace,
                            k -> new CollectionStats(namespace, shardName));
                    collStats.setServerMaxCacheBytes(serverMaxCacheBytes);

                    try {
                        Document collStatsCommand = new Document("collStats", collName);
                        Document collStatsDoc = client.getDatabase(dbName).runCommand(collStatsCommand);
                        collStats.updateFromCollStats(collStatsDoc);
                        successCount++;
                    } catch (Exception e) {
                        logger.warn("Error getting collStats for {} on shard {}: {}", namespace, shardName, e.getMessage());
                        failCount++;
                    }
                }
            }
            logger.debug("collStats update complete for shard {}: {} databases, {} collections, {} successful, {} failed",
                    shardName, dbCount, collCount, successCount, failCount);
        } catch (Exception e) {
            logger.error("Error updating collection stats for shard " + shardName, e);
        }
    }
    
    private int maxShardWidth = 20;
    private int maxCollectionWidth = 80;

    private synchronized void calculateColumnWidths() {
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
        StringBuilder header = new StringBuilder();
        header.append(String.format("%-8s %-" + maxShardWidth + "s %-" + maxCollectionWidth + "s %6s %6s %6s %6s %8s %8s",
                "Time", "Shard", "Collection", "ins", "qry", "upd", "del", "dataGB", "idxGB"));
        if (config.isIncludeCacheMb()) header.append(String.format(" %8s", "cacheMB"));
        if (config.isIncludeDirtyMb()) header.append(String.format(" %8s", "dirtyMB"));
        if (config.isCumulativeMode()) {
            header.append(String.format(" %8s %8s %7s %7s", "readGB", "writGB", "dirty%", "idxDty%"));
        } else {
            header.append(String.format(" %8s %8s %7s %7s", "readMB", "writMB", "dirty%", "idxDty%"));
        }
        System.out.println(header.toString());
        lineCount = 0;
    }

    private String formatDetailedLine(String timestamp, String shard, String namespace,
            Object ins, Object qry, Object upd, Object del,
            double dataGB, double idxGB, double cacheMB, double dirtyMB,
            double readMB, double writeMB, double dirtyPct, double idxDtyPct) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%-8s %-" + maxShardWidth + "s %-" + maxCollectionWidth + "s %6s %6s %6s %6s %8.2f %8.2f",
                timestamp, shard, namespace, ins, qry, upd, del, dataGB, idxGB));
        if (config.isIncludeCacheMb()) sb.append(String.format(" %8.1f", cacheMB));
        if (config.isIncludeDirtyMb()) sb.append(String.format(" %8.1f", dirtyMB));
        sb.append(String.format(" %8.1f %8.1f %6.1f%% %6.1f%%", readMB, writeMB, dirtyPct, idxDtyPct));
        return sb.toString();
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

    private Comparator<CollectionStats> getCollectionComparator() {
        String sortBy = config.getSortBy();

        switch (sortBy.toLowerCase()) {
            case "cachemb":
                return (cs1, cs2) -> {
                    double cache1 = cs1.getCacheCurrentBytes() != null ? cs1.getCacheCurrentBytes() : 0L;
                    double cache2 = cs2.getCacheCurrentBytes() != null ? cs2.getCacheCurrentBytes() : 0L;
                    return Double.compare(cache2, cache1); // Descending
                };
            case "dirtymb":
                return (cs1, cs2) -> {
                    double dirty1 = cs1.getCacheDirtyBytes() != null ? cs1.getCacheDirtyBytes() : 0L;
                    double dirty2 = cs2.getCacheDirtyBytes() != null ? cs2.getCacheDirtyBytes() : 0L;
                    return Double.compare(dirty2, dirty1); // Descending
                };
            case "datagb":
                return (cs1, cs2) -> {
                    double data1 = cs1.getDataSize() != null ? cs1.getDataSize() : 0L;
                    double data2 = cs2.getDataSize() != null ? cs2.getDataSize() : 0L;
                    return Double.compare(data2, data1); // Descending
                };
            case "idxgb":
                return (cs1, cs2) -> {
                    double idx1 = cs1.getIndexSize() != null ? cs1.getIndexSize() : 0L;
                    double idx2 = cs2.getIndexSize() != null ? cs2.getIndexSize() : 0L;
                    return Double.compare(idx2, idx1); // Descending
                };
            case "namespace":
                return (cs1, cs2) -> {
                    String ns1 = cs1.getNamespace() != null ? cs1.getNamespace() : "";
                    String ns2 = cs2.getNamespace() != null ? cs2.getNamespace() : "";
                    return ns1.compareTo(ns2); // Ascending
                };
            case "dirty%":
                return (cs1, cs2) -> {
                    double ratio1 = cs1.getDirtyFillRatio();
                    double ratio2 = cs2.getDirtyFillRatio();
                    return Double.compare(ratio2, ratio1); // Descending
                };
            case "idxdty%":
                return (cs1, cs2) -> {
                    double ratio1 = cs1.getIndexDirtyFillRatio();
                    double ratio2 = cs2.getIndexDirtyFillRatio();
                    return Double.compare(ratio2, ratio1); // Descending
                };
            case "readmb":
                return (cs1, cs2) -> {
                    double read1 = cs1.getDelta("cacheBytesRead");
                    double read2 = cs2.getDelta("cacheBytesRead");
                    return Double.compare(read2, read1); // Descending
                };
            case "writmb":
                return (cs1, cs2) -> {
                    double writ1 = cs1.getDelta("cacheBytesWritten");
                    double writ2 = cs2.getDelta("cacheBytesWritten");
                    return Double.compare(writ2, writ1); // Descending
                };
            default:
                logger.warn("Unknown sort field: {}, defaulting to cacheMB", sortBy);
                return (cs1, cs2) -> {
                    double cache1 = cs1.getCacheCurrentBytes() != null ? cs1.getCacheCurrentBytes() : 0L;
                    double cache2 = cs2.getCacheCurrentBytes() != null ? cs2.getCacheCurrentBytes() : 0L;
                    return Double.compare(cache2, cache1); // Descending
                };
        }
    }

    private Comparator<IndexStats> getIndexComparator() {
        String sortBy = config.getSortBy();

        switch (sortBy.toLowerCase()) {
            case "cachemb":
                return (idx1, idx2) -> {
                    double cache1 = idx1.getCacheCurrentBytes() != null ? idx1.getCacheCurrentBytes() : 0L;
                    double cache2 = idx2.getCacheCurrentBytes() != null ? idx2.getCacheCurrentBytes() : 0L;
                    return Double.compare(cache2, cache1); // Descending
                };
            case "dirtymb":
                return (idx1, idx2) -> {
                    double dirty1 = idx1.getCacheDirtyBytes() != null ? idx1.getCacheDirtyBytes() : 0L;
                    double dirty2 = idx2.getCacheDirtyBytes() != null ? idx2.getCacheDirtyBytes() : 0L;
                    return Double.compare(dirty2, dirty1); // Descending
                };
            case "idxgb":
                return (idx1, idx2) -> {
                    double size1 = idx1.getIndexSize() != null ? idx1.getIndexSize() : 0L;
                    double size2 = idx2.getIndexSize() != null ? idx2.getIndexSize() : 0L;
                    return Double.compare(size2, size1); // Descending
                };
            case "namespace":
                return (idx1, idx2) -> {
                    String name1 = idx1.getIndexName() != null ? idx1.getIndexName() : "";
                    String name2 = idx2.getIndexName() != null ? idx2.getIndexName() : "";
                    return name1.compareTo(name2); // Ascending
                };
            case "dirty%":
                return (idx1, idx2) -> {
                    double ratio1 = idx1.getDirtyFillRatio();
                    double ratio2 = idx2.getDirtyFillRatio();
                    return Double.compare(ratio2, ratio1); // Descending
                };
            case "readmb":
                return (idx1, idx2) -> {
                    double read1 = idx1.getDelta("cacheBytesRead");
                    double read2 = idx2.getDelta("cacheBytesRead");
                    return Double.compare(read2, read1); // Descending
                };
            case "writmb":
                return (idx1, idx2) -> {
                    double writ1 = idx1.getDelta("cacheBytesWritten");
                    double writ2 = idx2.getDelta("cacheBytesWritten");
                    return Double.compare(writ2, writ1); // Descending
                };
            default:
                // Default to cacheMB for indexes too
                return (idx1, idx2) -> {
                    double cache1 = idx1.getCacheCurrentBytes() != null ? idx1.getCacheCurrentBytes() : 0L;
                    double cache2 = idx2.getCacheCurrentBytes() != null ? idx2.getCacheCurrentBytes() : 0L;
                    return Double.compare(cache2, cache1); // Descending
                };
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

        // Calculate shard totals for read/write by summing collection values
        double totalReadVal = 0.0;
        double totalWriteVal = 0.0;
        if (shardCollStats != null && !shardCollStats.isEmpty()) {
            for (CollectionStats cs : shardCollStats.values()) {
                if (config.isCumulativeMode()) {
                    totalReadVal += cs.getCacheBytesRead() != null ? cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0 : 0.0;
                    totalWriteVal += cs.getCacheBytesWritten() != null ? cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0 : 0.0;
                } else {
                    totalReadVal += cs.getDelta("cacheBytesRead") / 1024.0 / 1024.0;
                    totalWriteVal += cs.getDelta("cacheBytesWritten") / 1024.0 / 1024.0;
                }
            }
        }

        // Print shard summary line
        System.out.println(formatDetailedLine(
                timestamp, shard, "[SHARD TOTAL]",
                status.getCurrentInserts(), status.getCurrentQueries(),
                status.getCurrentUpdates(), status.getCurrentDeletes(),
                0.0, 0.0,
                wtStats != null ? wtStats.getCurrentCacheBytes() / 1024.0 / 1024.0 : 0.0,
                wtStats != null ? wtStats.getDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                totalReadVal, totalWriteVal,
                wtStats != null ? wtStats.getDirtyFillRatio() * 100 : 0.0,
                0.0));
        lineCount++;

        // Print collection details if available
        if (shardCollStats != null && !shardCollStats.isEmpty()) {
            logger.debug("Processing {} collections for printing", shardCollStats.size());

            // Sort collections using configured comparator and apply top limit if specified
            Stream<CollectionStats> collectionStream = shardCollStats.values().stream()
                .sorted(getCollectionComparator());

            if (config.getTop() > 0) {
                collectionStream = collectionStream.limit(config.getTop());
            }

            collectionStream.forEach(cs -> {
                double readVal, writeVal;
                if (config.isCumulativeMode()) {
                    readVal = cs.getCacheBytesRead() != null ? cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0 : 0.0;
                    writeVal = cs.getCacheBytesWritten() != null ? cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0 : 0.0;
                } else {
                    readVal = cs.getDelta("cacheBytesRead") / 1024.0 / 1024.0;
                    writeVal = cs.getDelta("cacheBytesWritten") / 1024.0 / 1024.0;
                }
                double cacheMB = cs.getCacheCurrentBytes() != null ? cs.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;

                if (cacheMB < 0.05 && readVal < 0.05 && writeVal < 0.05) {
                    return;
                }

                System.out.println(formatDetailedLine(
                        timestamp, shard, cs.getNamespace(),
                        "-", "-", "-", "-",
                        cs.getDataSize() != null ? cs.getDataSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                        cs.getIndexSize() != null ? cs.getIndexSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                        cacheMB,
                        cs.getCacheDirtyBytes() != null ? cs.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                        readVal, writeVal,
                        cs.getDirtyFillRatio() * 100,
                        cs.getIndexDirtyFillRatio() * 100));
                lineCount++;

                // If includeIndexDetails is true, print each index as a separate row
                if (config.isIncludeIndexDetails() && cs.getIndexStats() != null && !cs.getIndexStats().isEmpty()) {
                    Stream<IndexStats> indexStream = cs.getIndexStats().values().stream()
                        .sorted(getIndexComparator());

                    if (config.getTop() > 0) {
                        indexStream = indexStream.limit(config.getTop());
                    }

                    indexStream.forEach(idx -> {
                        double idxCacheMB = idx.getCacheCurrentBytes() != null ? idx.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;
                        if (idxCacheMB < 0.05) {
                            return;
                        }
                        System.out.println(formatDetailedLine(
                                timestamp, shard, "  [idx] " + idx.getIndexName(),
                                "-", "-", "-", "-",
                                0.0,
                                idx.getIndexSize() != null ? idx.getIndexSize() / 1024.0 / 1024.0 / 1024.0 : 0.0,
                                idxCacheMB,
                                idx.getCacheDirtyBytes() != null ? idx.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0,
                                config.isCumulativeMode()
                                    ? (idx.getCacheBytesRead() != null ? idx.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0 : 0.0)
                                    : idx.getDelta("cacheBytesRead") / 1024.0 / 1024.0,
                                config.isCumulativeMode()
                                    ? (idx.getCacheBytesWritten() != null ? idx.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0 : 0.0)
                                    : idx.getDelta("cacheBytesWritten") / 1024.0 / 1024.0,
                                idx.getDirtyFillRatio() * 100,
                                0.0));
                        lineCount++;
                    });
                }
            });
        } else {
            logger.warn("No collection stats available for shard {} - map is {}", shard, shardCollStats != null ? "empty" : "null");
        }
    }

    private void printShardPivotReport() {
        String timestamp = LocalTime.now().format(timeFormatter);

        // Parse pivot metrics from config
        String[] metrics = config.getPivotMetrics().split(",");
        for (int i = 0; i < metrics.length; i++) {
            metrics[i] = metrics[i].trim();
        }

        // Collect all unique namespaces across all shards
        Set<String> allNamespaces = new TreeSet<>();
        for (Map<String, CollectionStats> shardStats : collectionStats.values()) {
            if (shardStats != null) {
                allNamespaces.addAll(shardStats.keySet());
            }
        }

        // Build a map of namespace -> shard -> stats for easier lookup
        Map<String, Map<String, CollectionStats>> namespaceToShardStats = new LinkedHashMap<>();
        for (String namespace : allNamespaces) {
            Map<String, CollectionStats> shardStats = new LinkedHashMap<>();
            for (Map.Entry<String, Map<String, CollectionStats>> entry : collectionStats.entrySet()) {
                String shardName = entry.getKey();
                Map<String, CollectionStats> collections = entry.getValue();
                if (collections != null && collections.containsKey(namespace)) {
                    shardStats.put(shardName, collections.get(namespace));
                }
            }
            namespaceToShardStats.put(namespace, shardStats);
        }

        // Sort by max²/avg_all_shards: rewards both high absolute activity and imbalance.
        // avg uses ALL shards (including zeros) so a single hot shard out of 10 gets ratio=10x,
        // not ratio=1x (which happens when zeros are excluded and count=1).
        final String sortMetric = config.getSortBy() != null ? config.getSortBy() : metrics[0];
        final int totalShards = shardClients.size();
        List<Map.Entry<String, Map<String, CollectionStats>>> sortedNamespaces = new ArrayList<>(namespaceToShardStats.entrySet());
        sortedNamespaces.sort((e1, e2) -> {
            double score1 = computePivotSortScore(e1.getValue(), sortMetric, totalShards);
            double score2 = computePivotSortScore(e2.getValue(), sortMetric, totalShards);
            return Double.compare(score2, score1);
        });

        // Apply top N limit if specified
        if (config.getTop() > 0 && sortedNamespaces.size() > config.getTop()) {
            sortedNamespaces = sortedNamespaces.subList(0, config.getTop());
        }

        // Get shard names in consistent order
        List<String> shardNames = new ArrayList<>(shardClients.keySet());

        // Calculate max width needed for each metric across all shards and collections
        // Add 1 to account for potential "*" prefix
        Map<String, Integer> metricWidths = new LinkedHashMap<>();
        for (String metric : metrics) {
            int maxWidth = metric.length(); // At least as wide as the metric name
            for (Map.Entry<String, Map<String, CollectionStats>> entry : sortedNamespaces) {
                for (String shardName : shardNames) {
                    CollectionStats cs = entry.getValue().get(shardName);
                    if (cs != null) {
                        double value = getMetricValue(cs, metric);
                        // Add 1 for potential "*" prefix
                        int valueWidth = String.format("%.0f", value).length() + 1;
                        if (valueWidth > maxWidth) {
                            maxWidth = valueWidth;
                        }
                    }
                }
            }
            metricWidths.put(metric, maxWidth);
        }

        // Calculate total width for each shard column (sum of metric widths + spaces)
        int shardColumnWidth = metricWidths.values().stream().mapToInt(Integer::intValue).sum() + (metrics.length - 1) * 2; // 2 spaces between metrics
        int namespaceWidth = 40;

        // Print header row 1: Shard names (centered)
        System.out.print(String.format("%-" + namespaceWidth + "s |", "Collection"));
        for (int i = 0; i < shardNames.size(); i++) {
            String shortName = "sh" + i;
            // Center the shard name
            int padding = (shardColumnWidth - shortName.length()) / 2;
            String centeredName = " ".repeat(padding) + shortName + " ".repeat(shardColumnWidth - padding - shortName.length());
            System.out.print(" " + centeredName + " |");
        }
        System.out.println();

        // Print header row 2: Metric labels (right-aligned within their columns)
        System.out.print(String.format("%-" + namespaceWidth + "s |", ""));
        for (int i = 0; i < shardNames.size(); i++) {
            System.out.print(" ");
            for (int j = 0; j < metrics.length; j++) {
                String metric = metrics[j];
                int width = metricWidths.get(metric);
                System.out.print(String.format("%" + width + "s", metric));
                if (j < metrics.length - 1) {
                    System.out.print("  "); // 2 spaces between metrics
                }
            }
            System.out.print(" |");
        }
        System.out.println();

        // Print separator line
        System.out.print("-".repeat(namespaceWidth) + "-+");
        for (int i = 0; i < shardNames.size(); i++) {
            System.out.print("-".repeat(shardColumnWidth + 2) + "+");
        }
        System.out.println();

        // In cumulative mode, print an uptime row so skewed counters are immediately visible
        if (config.isCumulativeMode()) {
            System.out.print(String.format("%-" + namespaceWidth + "s |", "[uptime hours]"));
            for (String shardName : shardNames) {
                Long uptimeSecs = shardUptimeSeconds.get(shardName);
                String uptimeStr = uptimeSecs != null ? String.format("%.1fh", uptimeSecs / 3600.0) : "?";
                // Center the value in the column
                int padding = Math.max(0, (shardColumnWidth - uptimeStr.length()) / 2);
                String centered = " ".repeat(padding) + uptimeStr + " ".repeat(Math.max(0, shardColumnWidth - padding - uptimeStr.length()));
                System.out.print(" " + centered + " |");
            }
            System.out.println();
            System.out.print("-".repeat(namespaceWidth) + "-+");
            for (int i = 0; i < shardNames.size(); i++) {
                System.out.print("-".repeat(shardColumnWidth + 2) + "+");
            }
            System.out.println();
        }

        // Print data rows
        for (Map.Entry<String, Map<String, CollectionStats>> entry : sortedNamespaces) {
            String namespace = entry.getKey();
            Map<String, CollectionStats> shardStats = entry.getValue();

            // Calculate average for each metric across all shards for this namespace
            // Exclude zero values to avoid diluting the average with empty/inactive shards
            Map<String, Double> metricAverages = new LinkedHashMap<>();
            for (String metric : metrics) {
                double sum = 0.0;
                int count = 0;
                for (String shardName : shardNames) {
                    CollectionStats cs = shardStats.get(shardName);
                    if (cs != null) {
                        double value = getMetricValue(cs, metric);
                        if (value > 0) {  // Only include non-zero values in average
                            sum += value;
                            count++;
                        }
                    }
                }
                double average = count > 0 ? sum / count : 0.0;
                metricAverages.put(metric, average);
            }

            // Truncate namespace if too long
            String displayNamespace = namespace.length() > namespaceWidth
                ? namespace.substring(0, namespaceWidth - 2) + ".."
                : namespace;

            System.out.print(String.format("%-" + namespaceWidth + "s |", displayNamespace));

            for (String shardName : shardNames) {
                CollectionStats cs = shardStats.get(shardName);
                System.out.print(" ");
                if (cs != null) {
                    for (int j = 0; j < metrics.length; j++) {
                        String metric = metrics[j];
                        int width = metricWidths.get(metric);
                        double value = getMetricValue(cs, metric);
                        double average = metricAverages.get(metric);

                        boolean hotHigh = average > 0 && value > average * 2.0  && value >= 0.5;
                        boolean hotLow  = average > 0 && value > average * 1.25 && value >= 0.5;
                        String prefix = hotLow ? "*" : "";
                        // Pad first so ANSI codes don't affect column alignment
                        String padded = String.format("%" + width + "s", prefix + String.format("%.0f", value));
                        if (!config.isNoColor() && value > 0) {
                            if (hotHigh) {
                                padded = "\033[91m" + padded + "\033[0m"; // bright red
                            } else if (hotLow) {
                                padded = "\033[33m" + padded + "\033[0m"; // yellow
                            }
                        }
                        System.out.print(padded);
                        if (j < metrics.length - 1) {
                            System.out.print("  "); // 2 spaces between metrics
                        }
                    }
                } else {
                    // Print empty spaces for missing data
                    for (int j = 0; j < metrics.length; j++) {
                        int width = metricWidths.get(metrics[j]);
                        System.out.print(" ".repeat(width));
                        if (j < metrics.length - 1) {
                            System.out.print("  ");
                        }
                    }
                }
                System.out.print(" |");
            }
            System.out.println();
        }

        System.out.println();
    }

    private double getMetricValue(CollectionStats cs, String metric) {
        switch (metric.toLowerCase()) {
            case "cachemb":
                return cs.getCacheCurrentBytes() != null ? cs.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;
            case "dirtymb":
                return cs.getCacheDirtyBytes() != null ? cs.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0;
            case "readmb":
                return cs.getDelta("cacheBytesRead") / 1024.0 / 1024.0;
            case "writmb":
            case "writemb":
                return cs.getDelta("cacheBytesWritten") / 1024.0 / 1024.0;
            case "readgb":
                return cs.getCacheBytesRead() != null ? cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "writgb":
            case "writegb":
                return cs.getCacheBytesWritten() != null ? cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "readgbh": {
                if (cs.getCacheBytesRead() == null) return 0.0;
                Long uptime = shardUptimeSeconds.get(cs.getShardName());
                if (uptime == null || uptime <= 0) return 0.0;
                return (cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0) / (uptime / 3600.0);
            }
            case "writgbh":
            case "writegbh": {
                if (cs.getCacheBytesWritten() == null) return 0.0;
                Long uptime = shardUptimeSeconds.get(cs.getShardName());
                if (uptime == null || uptime <= 0) return 0.0;
                return (cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0) / (uptime / 3600.0);
            }
            default:
                logger.warn("Unknown pivot metric: {}", metric);
                return 0.0;
        }
    }

    private double getMetricValueForSorting(CollectionStats cs, String metric) {
        switch (metric.toLowerCase()) {
            case "cachemb":
                return cs.getCacheCurrentBytes() != null ? cs.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;
            case "dirtymb":
                return cs.getCacheDirtyBytes() != null ? cs.getCacheDirtyBytes() / 1024.0 / 1024.0 : 0.0;
            case "datagb":
                return cs.getDataSize() != null ? cs.getDataSize() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "idxgb":
                return cs.getIndexSize() != null ? cs.getIndexSize() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "dirty%":
                return cs.getDirtyFillRatio() * 100;
            case "idxdty%":
                return cs.getIndexDirtyFillRatio() * 100;
            case "readmb":
                return cs.getDelta("cacheBytesRead") / 1024.0 / 1024.0;
            case "writmb":
            case "writemb":
                return cs.getDelta("cacheBytesWritten") / 1024.0 / 1024.0;
            case "readgb":
                return cs.getCacheBytesRead() != null ? cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "writgb":
            case "writegb":
                return cs.getCacheBytesWritten() != null ? cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0 : 0.0;
            case "readgbh": {
                if (cs.getCacheBytesRead() == null) return 0.0;
                Long uptime = shardUptimeSeconds.get(cs.getShardName());
                if (uptime == null || uptime <= 0) return 0.0;
                return (cs.getCacheBytesRead() / 1024.0 / 1024.0 / 1024.0) / (uptime / 3600.0);
            }
            case "writgbh":
            case "writegbh": {
                if (cs.getCacheBytesWritten() == null) return 0.0;
                Long uptime = shardUptimeSeconds.get(cs.getShardName());
                if (uptime == null || uptime <= 0) return 0.0;
                return (cs.getCacheBytesWritten() / 1024.0 / 1024.0 / 1024.0) / (uptime / 3600.0);
            }
            case "namespace":
                return 0.0; // Namespace sorting not applicable for numeric sorting
            default:
                logger.warn("Unknown sort metric: {}, defaulting to cacheMB", metric);
                return cs.getCacheCurrentBytes() != null ? cs.getCacheCurrentBytes() / 1024.0 / 1024.0 : 0.0;
        }
    }

    private double computePivotSortScore(Map<String, CollectionStats> shardStats, String metric, int totalShards) {
        double sum = 0.0;
        double max = 0.0;
        for (CollectionStats cs : shardStats.values()) {
            double value = getMetricValueForSorting(cs, metric);
            if (value > 0) {
                sum += value;
                if (value > max) max = value;
            }
        }
        if (sum == 0 || max == 0) return 0.0;
        // avg over ALL shards (including zeros): single hot shard out of 10 → ratio=10, not 1
        double avgAll = sum / totalShards;
        return max * (max / avgAll); // max² / avg_all = max × imbalance_ratio
    }

    /**
     * Extract shard identifier from full shard name.
     * Examples:
     *   atlas-pzgfjj-shard-0 -> shard-0
     *   atlas-pzgfjj-shard-02 -> shard-02
     *   fooBlahBlahShard1 -> shard1
     *   configServer -> configServer (no match, return as-is)
     */
    private String extractShardName(String fullName) {
        if (fullName == null) {
            return "unknown";
        }

        // Try to find "shard" (case insensitive) followed by optional separator and digits
        // Pattern matches: shard-0, shard_0, shard0, Shard1, etc.
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("(shard[-_]?\\d+)", java.util.regex.Pattern.CASE_INSENSITIVE);
        java.util.regex.Matcher matcher = pattern.matcher(fullName);

        if (matcher.find()) {
            return matcher.group(1).toLowerCase();
        }

        // No shard pattern found, return the original name (will be truncated if too long)
        return fullName;
    }

    private void runJsonOutputInternal(boolean shouldOutput) {
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
                    String shardName = getShardNameForClient(client);
                    shardNode.put("shard", shardName != null ? shardName : "unknown");

                    ServerStatus status = null;
                    Document serverStatus = null;

                    // Try to get serverStatus, but handle authorization errors gracefully
                    if (config.getManualCacheSizeBytes() == null) {
                        try {
                            serverStatus = client.getDatabase("admin").runCommand(new Document("serverStatus", 1));

                            if (serverStatuses.size() >= currentIndex + 1) {
                                status = serverStatuses.get(currentIndex);
                            } else {
                                status = new ServerStatus();
                                serverStatuses.add(currentIndex, status);
                            }
                            status.updateServerStatus(serverStatus);

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
                        } catch (com.mongodb.MongoCommandException e) {
                            if (e.getErrorCode() == 13) { // Unauthorized
                                logger.warn("Not authorized to run serverStatus on {}. Use --cache-size-gb to specify cache size manually.", shardName);
                                // Create empty status
                                if (serverStatuses.size() >= currentIndex + 1) {
                                    status = serverStatuses.get(currentIndex);
                                } else {
                                    status = new ServerStatus();
                                    serverStatuses.add(currentIndex, status);
                                }
                            } else {
                                throw e;
                            }
                        }
                    } else {
                        // Using manual cache size, skip serverStatus
                        logger.debug("Using manual cache size, skipping serverStatus for {}", shardName);
                        if (serverStatuses.size() >= currentIndex + 1) {
                            status = serverStatuses.get(currentIndex);
                        } else {
                            status = new ServerStatus();
                            serverStatuses.add(currentIndex, status);
                        }

                        // Set manual cache size on WiredTiger stats
                        if (config.isIncludeWiredTigerStats() && shardName != null) {
                            WiredTigerCacheStats wtStats = wtCacheStats.get(shardName);
                            if (wtStats != null) {
                                wtStats.setManualMaxCacheBytes(config.getManualCacheSizeBytes());
                                addWtCacheStatsToJson(shardNode, wtStats);
                            }
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

        if (shouldOutput) {
            try {
                System.out.println(objectMapper.writeValueAsString(rootNode));
            } catch (Exception e) {
                logger.error("Error serializing JSON output", e);
            }
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
