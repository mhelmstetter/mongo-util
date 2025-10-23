package com.mongodb.healthcheck;

import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.healthcheck.ClusterIndexStats.CollectionStats;
import com.mongodb.healthcheck.ClusterIndexStats.DatabaseStats;
import com.mongodb.healthcheck.ClusterIndexStats.IndexInfo;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Collects index usage statistics from a MongoDB cluster
 */
public class IndexStatsCollector {

    private static final Logger logger = LoggerFactory.getLogger(IndexStatsCollector.class);

    private final MongoClient client;
    private final int lowUsageThreshold;
    private Map<String, String> shardKeyMap;

    public IndexStatsCollector(MongoClient client, int lowUsageThreshold) {
        this.client = client;
        this.lowUsageThreshold = lowUsageThreshold;
    }

    public ClusterIndexStats collect() {
        logger.info("Starting index statistics collection");

        // Check if cluster is sharded
        boolean isSharded = checkIfSharded();
        logger.info("Cluster is sharded: {}", isSharded);

        ClusterIndexStats stats = new ClusterIndexStats(isSharded, lowUsageThreshold);

        // Load shard keys if cluster is sharded
        if (isSharded) {
            loadShardKeys();
        }

        // Get list of databases
        MongoIterable<String> databaseNames = client.listDatabaseNames();

        for (String dbName : databaseNames) {
            // Skip system databases
            if (dbName.equals("admin") || dbName.equals("config") || dbName.equals("local")) {
                logger.debug("Skipping system database: {}", dbName);
                continue;
            }

            logger.info("Processing database: {}", dbName);
            DatabaseStats dbStats = processDatabase(dbName);
            stats.addDatabase(dbStats);
        }

        logger.info("Index statistics collection complete");
        logger.info("Total indexes: {}", stats.getTotalIndexCount());
        logger.info("Unused indexes: {}", stats.getUnusedIndexCount());
        logger.info("Low usage indexes: {}", stats.getLowUsageIndexCount());

        return stats;
    }

    private boolean checkIfSharded() {
        try {
            MongoDatabase configDb = client.getDatabase("config");
            // Try to access config.shards - if it exists and has data, cluster is sharded
            long shardCount = configDb.getCollection("shards").countDocuments();
            return shardCount > 0;
        } catch (Exception e) {
            logger.debug("Could not access config.shards, assuming non-sharded cluster", e);
            return false;
        }
    }

    private void loadShardKeys() {
        logger.info("Loading shard keys from config.collections");
        shardKeyMap = new HashMap<>();

        try {
            MongoDatabase configDb = client.getDatabase("config");
            MongoCollection<Document> collections = configDb.getCollection("collections");

            for (Document collDoc : collections.find()) {
                String namespace = collDoc.getString("_id");
                Document key = collDoc.get("key", Document.class);
                if (key != null) {
                    String shardKey = key.toJson();
                    shardKeyMap.put(namespace, shardKey);
                    logger.debug("Shard key for {}: {}", namespace, shardKey);
                }
            }

            logger.info("Loaded {} shard keys", shardKeyMap.size());
        } catch (Exception e) {
            logger.error("Error loading shard keys", e);
            shardKeyMap = new HashMap<>();
        }
    }

    private DatabaseStats processDatabase(String dbName) {
        DatabaseStats dbStats = new DatabaseStats(dbName);
        MongoDatabase database = client.getDatabase(dbName);

        // List collections in the database
        MongoIterable<String> collectionNames = database.listCollectionNames();

        for (String collName : collectionNames) {
            // Skip system collections
            if (collName.startsWith("system.")) {
                logger.debug("Skipping system collection: {}.{}", dbName, collName);
                continue;
            }

            String namespace = dbName + "." + collName;
            logger.debug("Processing collection: {}", namespace);

            String shardKey = shardKeyMap != null ? shardKeyMap.get(namespace) : null;
            CollectionStats collStats = processCollection(database, dbName, collName, shardKey);
            dbStats.addCollection(collStats);
        }

        return dbStats;
    }

    private CollectionStats processCollection(MongoDatabase database, String dbName,
                                              String collName, String shardKey) {
        CollectionStats collStats = new CollectionStats(dbName, collName, shardKey);
        String namespace = dbName + "." + collName;

        try {
            MongoCollection<Document> collection = database.getCollection(collName);

            // Map to aggregate stats by index name: indexName -> AggregatedIndexStats
            Map<String, AggregatedIndexStats> indexStatsMap = new HashMap<>();

            // Collect stats from primary
            collectIndexStatsFromReadPreference(collection, namespace, indexStatsMap,
                ReadPreference.primary(), true);

            // Collect stats from secondary (use secondaryPreferred in case no secondary exists)
            collectIndexStatsFromReadPreference(collection, namespace, indexStatsMap,
                ReadPreference.secondaryPreferred(), false);

            // Get index sizes from collection stats
            Map<String, Long> indexSizes = getIndexSizes(database, collName);

            // Convert aggregated stats to IndexInfo objects
            for (Map.Entry<String, AggregatedIndexStats> entry : indexStatsMap.entrySet()) {
                String indexName = entry.getKey();
                AggregatedIndexStats aggStats = entry.getValue();

                // Get size for this index
                long sizeBytes = indexSizes.getOrDefault(indexName, 0L);

                // Check if this index matches the shard key
                boolean isShardKey = false;
                if (shardKey != null && aggStats.keyPattern.equals(shardKey)) {
                    isShardKey = true;
                    logger.debug("Index {} is the shard key for {}", indexName, namespace);
                }

                IndexInfo indexInfo = new IndexInfo(
                    namespace, indexName, aggStats.keyPattern,
                    aggStats.primaryAccesses, aggStats.secondaryAccesses,
                    aggStats.earliestSince, sizeBytes, isShardKey, lowUsageThreshold
                );

                collStats.addIndex(indexInfo);
            }

        } catch (Exception e) {
            logger.error("Error processing collection {}: {}", namespace, e.getMessage());
        }

        return collStats;
    }

    private void collectIndexStatsFromReadPreference(MongoCollection<Document> collection,
                                                     String namespace,
                                                     Map<String, AggregatedIndexStats> indexStatsMap,
                                                     ReadPreference readPreference,
                                                     boolean isPrimary) {
        try {
            MongoCollection<Document> collectionWithRP = collection.withReadPreference(readPreference);

            for (Document indexStat : collectionWithRP.aggregate(
                    java.util.Arrays.asList(new Document("$indexStats", new Document())))) {

                String indexName = indexStat.getString("name");
                Document key = indexStat.get("key", Document.class);
                String keyPattern = formatKeyPatternAsJson(key);

                // Get access stats
                Document accesses = indexStat.get("accesses", Document.class);
                long ops = accesses != null ? accesses.getLong("ops") : 0L;
                Date since = accesses != null ? accesses.getDate("since") : new Date();

                // Get or create aggregated stats for this index
                AggregatedIndexStats aggStats = indexStatsMap.computeIfAbsent(
                    indexName, k -> new AggregatedIndexStats(keyPattern));

                // Add the accesses to the appropriate counter
                if (isPrimary) {
                    aggStats.primaryAccesses += ops;
                } else {
                    aggStats.secondaryAccesses += ops;
                }

                // Keep the earliest "since" date
                if (since != null && (aggStats.earliestSince == null || since.before(aggStats.earliestSince))) {
                    aggStats.earliestSince = since;
                }
            }
        } catch (Exception e) {
            logger.debug("Could not collect stats from {}: {}", readPreference, e.getMessage());
        }
    }

    private Map<String, Long> getIndexSizes(MongoDatabase database, String collName) {
        Map<String, Long> sizes = new HashMap<>();
        try {
            // Run collStats to get index sizes
            Document collStats = database.runCommand(new Document("collStats", collName));
            Document indexSizes = collStats.get("indexSizes", Document.class);
            if (indexSizes != null) {
                for (String indexName : indexSizes.keySet()) {
                    Object sizeObj = indexSizes.get(indexName);
                    if (sizeObj instanceof Number) {
                        sizes.put(indexName, ((Number) sizeObj).longValue());
                    }
                }
            }
        } catch (Exception e) {
            logger.debug("Could not get index sizes for {}.{}: {}", database.getName(), collName, e.getMessage());
        }
        return sizes;
    }

    /**
     * Formats a BSON Document key pattern as clean JSON string
     * Example: {"context": 1, "name": 1, "updateDate": -1, "properties.promotionStatus": 1}
     */
    private String formatKeyPatternAsJson(Document key) {
        if (key == null || key.isEmpty()) {
            return "{}";
        }

        StringBuilder json = new StringBuilder("{");
        boolean first = true;

        for (Map.Entry<String, Object> entry : key.entrySet()) {
            if (!first) {
                json.append(", ");
            }
            first = false;

            // Add field name in quotes
            json.append("\"").append(entry.getKey()).append("\": ");

            // Add value (usually 1, -1, "hashed", "text", etc.)
            Object value = entry.getValue();
            if (value instanceof String) {
                json.append("\"").append(value).append("\"");
            } else {
                json.append(value);
            }
        }

        json.append("}");
        return json.toString();
    }

    // Helper class to aggregate index statistics across multiple nodes/shards
    private static class AggregatedIndexStats {
        String keyPattern;
        long primaryAccesses = 0;
        long secondaryAccesses = 0;
        Date earliestSince = null;

        AggregatedIndexStats(String keyPattern) {
            this.keyPattern = keyPattern;
        }
    }
}
