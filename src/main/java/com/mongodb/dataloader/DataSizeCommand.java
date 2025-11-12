package com.mongodb.dataloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "dataSize",
         mixinStandardHelpOptions = true,
         description = "Display data size per shard")
public class DataSizeCommand implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(DataSizeCommand.class);

    @Option(names = {"--uri"}, required = true, description = "MongoDB connection URI (mongos)")
    private String uri;

    @Option(names = {"--database"}, description = "Filter by database name (optional)")
    private String databaseFilter;

    private ShardClient shardClient;

    @Override
    public Integer call() throws Exception {
        logger.info("Analyzing data sizes across shards");

        try {
            // Initialize ShardClient
            shardClient = new ShardClient("dataloader", uri);
            shardClient.init();

            // Get shard data sizes (works for both sharded and non-sharded deployments)
            Map<String, ShardDataSize> shardSizes = collectShardDataSizes();

            // Display results in table format
            displayShardSizeTable(shardSizes);

            return 0;

        } catch (Exception e) {
            logger.error("Error during data size analysis", e);
            return 1;
        } finally {
            if (shardClient != null && shardClient.getMongoClient() != null) {
                shardClient.getMongoClient().close();
            }
        }
    }

    private Map<String, ShardDataSize> collectShardDataSizes() {
        Map<String, ShardDataSize> shardSizes = new TreeMap<>();

        // For sharded clusters, initialize with known shards
        // For non-sharded deployments, this will be empty and populated during dbStats parsing
        if (shardClient.isMongos()) {
            Map<String, Shard> shardsMap = shardClient.getShardsMap();
            logger.info("Found {} shards in cluster", shardsMap.size());

            // Initialize shard size tracking for all known shards
            for (String shardName : shardsMap.keySet()) {
                shardSizes.put(shardName, new ShardDataSize(shardName, 0, 0, 0, new HashMap<>()));
            }
        } else {
            logger.info("Non-sharded deployment detected");
        }

        // Get database list
        MongoClient mongosClient = shardClient.getMongoClient();
        MongoIterable<String> dbNames = mongosClient.listDatabaseNames();

        // For each database, run dbStats through mongos to get per-shard statistics
        for (String dbName : dbNames) {
            // Skip system databases
            if (dbName.equals("admin") || dbName.equals("config") || dbName.equals("local")) {
                continue;
            }

            // Apply database filter if specified
            if (databaseFilter != null && !dbName.equals(databaseFilter)) {
                continue;
            }

            try {
                // Run dbStats through mongos - it returns per-shard statistics in the "raw" field
                Document dbStats = mongosClient.getDatabase(dbName).runCommand(new Document("dbStats", 1));

                logger.debug("dbStats for {}: {}", dbName, dbStats.toJson());

                // For sharded clusters, dbStats returns a "raw" field with per-shard statistics
                // For non-sharded deployments, there's no "raw" field
                Document rawDoc = dbStats.get("raw", Document.class);
                if (rawDoc != null) {
                    // Sharded cluster - parse per-shard stats
                    for (String shardKey : rawDoc.keySet()) {
                        Document shardStats = rawDoc.get(shardKey, Document.class);
                        if (shardStats != null) {
                            // The key is like "atlas-o3y5yd-shard-0/host1:port,host2:port,..."
                            // Extract just the shard name (part before the '/')
                            String shardName = shardKey.contains("/") ? shardKey.substring(0, shardKey.indexOf("/")) : shardKey;
                            updateShardDataSize(shardSizes, shardName, dbName, shardStats);
                        }
                    }
                } else {
                    // Non-sharded deployment - use the top-level stats
                    // For non-sharded, we'll use a single "standalone" entry
                    if (shardSizes.isEmpty()) {
                        shardSizes.put("standalone", new ShardDataSize("standalone", 0, 0, 0, new HashMap<>()));
                    }
                    updateShardDataSize(shardSizes, "standalone", dbName, dbStats);
                }
            } catch (Exception e) {
                logger.warn("Failed to get stats for database {}: {}", dbName, e.getMessage());
            }
        }

        return shardSizes;
    }

    /**
     * Updates the shard data size tracking with statistics from a database on a specific shard.
     * This method accumulates statistics across multiple databases for each shard.
     */
    private void updateShardDataSize(Map<String, ShardDataSize> shardSizes, String shardName,
                                     String dbName, Document shardStats) {
        // Extract statistics from the shard's dbStats
        long dataSize = 0;
        Object dataSizeObj = shardStats.get("dataSize");
        if (dataSizeObj instanceof Number) {
            dataSize = ((Number) dataSizeObj).longValue();
        }

        long collectionCount = 0;
        Object collectionsObj = shardStats.get("collections");
        if (collectionsObj instanceof Number) {
            collectionCount = ((Number) collectionsObj).longValue();
        }

        long documentCount = 0;
        Object objectsObj = shardStats.get("objects");
        if (objectsObj instanceof Number) {
            documentCount = ((Number) objectsObj).longValue();
        }

        logger.debug("Shard {}, DB {}: {} bytes ({} collections, {} documents)",
                    shardName, dbName, dataSize, collectionCount, documentCount);

        // Get or create the ShardDataSize entry
        ShardDataSize existing = shardSizes.get(shardName);
        if (existing == null) {
            // This shard wasn't in our initial list - add it
            Map<String, Long> dbSizes = new HashMap<>();
            dbSizes.put(dbName, dataSize);
            shardSizes.put(shardName, new ShardDataSize(shardName, dataSize, collectionCount, documentCount, dbSizes));
        } else {
            // Accumulate statistics for this database
            Map<String, Long> dbSizes = new HashMap<>(existing.databaseSizes);
            dbSizes.put(dbName, dataSize);

            ShardDataSize updated = new ShardDataSize(
                shardName,
                existing.totalSize + dataSize,
                existing.collectionCount + collectionCount,
                existing.documentCount + documentCount,
                dbSizes
            );
            shardSizes.put(shardName, updated);
        }
    }

    private void displayShardSizeTable(Map<String, ShardDataSize> shardSizes) {
        System.out.println();
        System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
        System.out.println("║              Data Size Per Shard Analysis                            ║");
        System.out.println("╠══════════════════════════════════════════════════════════════════════╣");
        System.out.println();

        // Calculate totals
        long grandTotalSize = 0;
        long grandTotalCollections = 0;
        long grandTotalDocuments = 0;

        for (ShardDataSize size : shardSizes.values()) {
            grandTotalSize += size.totalSize;
            grandTotalCollections += size.collectionCount;
            grandTotalDocuments += size.documentCount;
        }

        // Determine best unit based on total size
        String unit;
        double divisor;
        if (grandTotalSize >= 1024L * 1024L * 1024L * 1024L) {
            unit = "TB";
            divisor = 1024.0 * 1024.0 * 1024.0 * 1024.0;
        } else if (grandTotalSize >= 1024L * 1024L * 1024L) {
            unit = "GB";
            divisor = 1024.0 * 1024.0 * 1024.0;
        } else if (grandTotalSize >= 1024L * 1024L) {
            unit = "MB";
            divisor = 1024.0 * 1024.0;
        } else if (grandTotalSize >= 1024L) {
            unit = "KB";
            divisor = 1024.0;
        } else {
            unit = "B";
            divisor = 1.0;
        }

        // Find max shard name length for formatting
        int maxShardNameLen = shardSizes.keySet().stream()
            .mapToInt(String::length)
            .max()
            .orElse(10);
        maxShardNameLen = Math.max(maxShardNameLen, 10);

        // Header
        String sizeHeader = "Size (" + unit + ")";
        String format = "  %-" + maxShardNameLen + "s  %15s  %15s  %12s\n";
        System.out.printf(format, "Shard", sizeHeader, "Documents", "Collections");
        System.out.println("  " + "─".repeat(maxShardNameLen + 2 + 15 + 2 + 15 + 2 + 12));

        // Shard rows
        for (Map.Entry<String, ShardDataSize> entry : shardSizes.entrySet()) {
            String shardName = entry.getKey();
            ShardDataSize size = entry.getValue();

            double sizeInUnit = size.totalSize / divisor;
            String sizeStr = String.format("%.2f", sizeInUnit);

            System.out.printf(format, shardName, sizeStr,
                            String.format("%,d", size.documentCount),
                            size.collectionCount);
        }

        // Summary
        System.out.println("  " + "─".repeat(maxShardNameLen + 2 + 15 + 2 + 15 + 2 + 12));

        double totalSizeInUnit = grandTotalSize / divisor;
        System.out.printf(format, "TOTAL",
                         String.format("%.2f", totalSizeInUnit),
                         String.format("%,d", grandTotalDocuments),
                         grandTotalCollections);

        System.out.println();

        // Show balance statistics
        if (shardSizes.size() > 1) {
            double avgSize = (double) grandTotalSize / shardSizes.size();
            double maxDeviation = 0;
            String mostImbalancedShard = "";

            for (Map.Entry<String, ShardDataSize> entry : shardSizes.entrySet()) {
                double deviation = Math.abs(entry.getValue().totalSize - avgSize) / avgSize * 100;
                if (deviation > maxDeviation) {
                    maxDeviation = deviation;
                    mostImbalancedShard = entry.getKey();
                }
            }

            double avgSizeInUnit = avgSize / divisor;
            System.out.println("  Balance Statistics:");
            System.out.printf("    Average size per shard: %.2f %s\n", avgSizeInUnit, unit);
            System.out.printf("    Max deviation: %.1f%% (%s)\n", maxDeviation, mostImbalancedShard);

            if (maxDeviation < 10) {
                System.out.println("    Status: ✓ Well balanced");
            } else if (maxDeviation < 25) {
                System.out.println("    Status: ⚠ Moderately imbalanced");
            } else {
                System.out.println("    Status: ✗ Significantly imbalanced");
            }
        }

        System.out.println();
        System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
        System.out.println();
    }

    private static class ShardDataSize {
        final String shardName;
        final long totalSize;
        final long collectionCount;
        final long documentCount;
        final Map<String, Long> databaseSizes;

        ShardDataSize(String shardName, long totalSize, long collectionCount, long documentCount, Map<String, Long> databaseSizes) {
            this.shardName = shardName;
            this.totalSize = totalSize;
            this.collectionCount = collectionCount;
            this.documentCount = documentCount;
            this.databaseSizes = databaseSizes;
        }
    }
}
