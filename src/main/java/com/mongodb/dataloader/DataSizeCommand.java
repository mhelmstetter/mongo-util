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

            if (!shardClient.isMongos()) {
                logger.error("Error: --uri must point to a mongos instance for shard analysis");
                return 1;
            }

            // Get shard data sizes
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
        Map<String, Shard> shardsMap = shardClient.getShardsMap();

        logger.info("Found {} shards in cluster", shardsMap.size());

        // Connect to each shard and collect data sizes
        for (Map.Entry<String, Shard> entry : shardsMap.entrySet()) {
            String shardName = entry.getKey();
            Shard shard = entry.getValue();
            String shardUri = shard.getHost();

            logger.debug("Connecting to shard: {} at {}", shardName, shardUri);

            try {
                ShardDataSize dataSize = getShardDataSize(shardName, shardUri);
                shardSizes.put(shardName, dataSize);
            } catch (Exception e) {
                logger.warn("Failed to get data size for shard {}: {}", shardName, e.getMessage());
                shardSizes.put(shardName, new ShardDataSize(shardName, 0, 0, 0, new HashMap<>()));
            }
        }

        return shardSizes;
    }

    private ShardDataSize getShardDataSize(String shardName, String shardUri) {
        // Parse shard URI to get replica set connection string
        // Format is typically "rsname/host1:port1,host2:port2"
        String connectionString;
        if (shardUri.contains("/")) {
            String[] parts = shardUri.split("/", 2);
            connectionString = "mongodb://" + parts[1];
        } else {
            connectionString = "mongodb://" + shardUri;
        }

        try (MongoClient shardClient = com.mongodb.client.MongoClients.create(connectionString)) {
            long totalSize = 0;
            long totalCollections = 0;
            long totalDocuments = 0;
            Map<String, Long> databaseSizes = new HashMap<>();

            // Get list of databases
            MongoIterable<String> dbNames = shardClient.listDatabaseNames();

            for (String dbName : dbNames) {
                // Skip system databases
                if (dbName.equals("admin") || dbName.equals("config") || dbName.equals("local")) {
                    continue;
                }

                // Apply database filter if specified
                if (databaseFilter != null && !dbName.equals(databaseFilter)) {
                    continue;
                }

                // Get database stats
                Document dbStats = shardClient.getDatabase(dbName).runCommand(new Document("dbStats", 1));

                // MongoDB returns dataSize as a Double, need to convert safely
                long dataSize = 0;
                Object dataSizeObj = dbStats.get("dataSize");
                if (dataSizeObj instanceof Number) {
                    dataSize = ((Number) dataSizeObj).longValue();
                }

                // collections is typically an Integer
                long collectionCount = 0;
                Object collectionsObj = dbStats.get("collections");
                if (collectionsObj instanceof Number) {
                    collectionCount = ((Number) collectionsObj).longValue();
                }

                // objects field contains document count
                long documentCount = 0;
                Object objectsObj = dbStats.get("objects");
                if (objectsObj instanceof Number) {
                    documentCount = ((Number) objectsObj).longValue();
                }

                databaseSizes.put(dbName, dataSize);
                totalSize += dataSize;
                totalCollections += collectionCount;
                totalDocuments += documentCount;

                logger.debug("Shard {}, DB {}: {} bytes ({} collections, {} documents)",
                            shardName, dbName, dataSize, collectionCount, documentCount);
            }

            return new ShardDataSize(shardName, totalSize, totalCollections, totalDocuments, databaseSizes);

        } catch (Exception e) {
            logger.error("Error connecting to shard {}: {}", shardName, e.getMessage());
            throw e;
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

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / (1024.0 * 1024.0));
        } else if (bytes < 1024L * 1024L * 1024L * 1024L) {
            return String.format("%.2f GB", bytes / (1024.0 * 1024.0 * 1024.0));
        } else {
            return String.format("%.2f TB", bytes / (1024.0 * 1024.0 * 1024.0 * 1024.0));
        }
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
