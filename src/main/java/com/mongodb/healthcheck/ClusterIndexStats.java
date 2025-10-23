package com.mongodb.healthcheck;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Holds index statistics for an entire MongoDB cluster
 */
public class ClusterIndexStats {

    private final List<DatabaseStats> databases = new ArrayList<>();
    private final boolean isSharded;
    private final int lowUsageThreshold;

    public ClusterIndexStats(boolean isSharded, int lowUsageThreshold) {
        this.isSharded = isSharded;
        this.lowUsageThreshold = lowUsageThreshold;
    }

    public void addDatabase(DatabaseStats dbStats) {
        databases.add(dbStats);
    }

    public List<DatabaseStats> getDatabases() {
        return databases;
    }

    public boolean isSharded() {
        return isSharded;
    }

    public int getTotalIndexCount() {
        return databases.stream()
            .mapToInt(DatabaseStats::getTotalIndexCount)
            .sum();
    }

    public int getUnusedIndexCount() {
        return databases.stream()
            .mapToInt(DatabaseStats::getUnusedIndexCount)
            .sum();
    }

    public int getLowUsageIndexCount() {
        return databases.stream()
            .mapToInt(DatabaseStats::getLowUsageIndexCount)
            .sum();
    }

    public List<IndexInfo> getUnusedIndexes() {
        List<IndexInfo> unused = new ArrayList<>();
        for (DatabaseStats db : databases) {
            for (CollectionStats coll : db.getCollections()) {
                for (IndexInfo idx : coll.getIndexes()) {
                    if (idx.isUnused() && !idx.isShardKey()) {
                        unused.add(idx);
                    }
                }
            }
        }
        return unused;
    }

    public List<IndexInfo> getLowUsageIndexes() {
        List<IndexInfo> lowUsage = new ArrayList<>();
        for (DatabaseStats db : databases) {
            for (CollectionStats coll : db.getCollections()) {
                for (IndexInfo idx : coll.getIndexes()) {
                    if (idx.isLowUsage() && !idx.isUnused() && !idx.isShardKey()) {
                        lowUsage.add(idx);
                    }
                }
            }
        }
        return lowUsage;
    }

    public static class DatabaseStats {
        private final String name;
        private final List<CollectionStats> collections = new ArrayList<>();

        public DatabaseStats(String name) {
            this.name = name;
        }

        public void addCollection(CollectionStats collStats) {
            collections.add(collStats);
        }

        public String getName() {
            return name;
        }

        public List<CollectionStats> getCollections() {
            return collections;
        }

        public int getTotalIndexCount() {
            return collections.stream()
                .mapToInt(c -> c.getIndexes().size())
                .sum();
        }

        public int getUnusedIndexCount() {
            return collections.stream()
                .mapToInt(CollectionStats::getUnusedIndexCount)
                .sum();
        }

        public int getLowUsageIndexCount() {
            return collections.stream()
                .mapToInt(CollectionStats::getLowUsageIndexCount)
                .sum();
        }
    }

    public static class CollectionStats {
        private final String databaseName;
        private final String collectionName;
        private final String shardKey;
        private final List<IndexInfo> indexes = new ArrayList<>();

        public CollectionStats(String databaseName, String collectionName, String shardKey) {
            this.databaseName = databaseName;
            this.collectionName = collectionName;
            this.shardKey = shardKey;
        }

        public void addIndex(IndexInfo indexInfo) {
            indexes.add(indexInfo);
        }

        public String getDatabaseName() {
            return databaseName;
        }

        public String getCollectionName() {
            return collectionName;
        }

        public String getNamespace() {
            return databaseName + "." + collectionName;
        }

        public String getShardKey() {
            return shardKey;
        }

        public List<IndexInfo> getIndexes() {
            return indexes;
        }

        public int getUnusedIndexCount() {
            return (int) indexes.stream()
                .filter(idx -> idx.isUnused() && !idx.isShardKey())
                .count();
        }

        public int getLowUsageIndexCount() {
            return (int) indexes.stream()
                .filter(idx -> idx.isLowUsage() && !idx.isUnused() && !idx.isShardKey())
                .count();
        }
    }

    public static class IndexInfo {
        private final String namespace;
        private final String name;
        private final String keyPattern;
        private final long primaryAccesses;
        private final long secondaryAccesses;
        private final Date since;
        private final long sizeBytes;
        private final boolean isShardKey;
        private final int lowUsageThreshold;

        public IndexInfo(String namespace, String name, String keyPattern,
                        long primaryAccesses, long secondaryAccesses,
                        Date since, long sizeBytes, boolean isShardKey, int lowUsageThreshold) {
            this.namespace = namespace;
            this.name = name;
            this.keyPattern = keyPattern;
            this.primaryAccesses = primaryAccesses;
            this.secondaryAccesses = secondaryAccesses;
            this.since = since;
            this.sizeBytes = sizeBytes;
            this.isShardKey = isShardKey;
            this.lowUsageThreshold = lowUsageThreshold;
        }

        public String getNamespace() {
            return namespace;
        }

        public String getName() {
            return name;
        }

        public String getKeyPattern() {
            return keyPattern;
        }

        public long getPrimaryAccesses() {
            return primaryAccesses;
        }

        public long getSecondaryAccesses() {
            return secondaryAccesses;
        }

        public long getTotalAccesses() {
            return primaryAccesses + secondaryAccesses;
        }

        public Date getSince() {
            return since;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }

        public double getSizeMB() {
            return sizeBytes / (1024.0 * 1024.0);
        }

        public boolean isShardKey() {
            return isShardKey;
        }

        public boolean isUnused() {
            return getTotalAccesses() == 0;
        }

        public boolean isLowUsage() {
            long total = getTotalAccesses();
            return total > 0 && total < lowUsageThreshold;
        }
    }
}
