package com.mongodb.custombalancer.core;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonDocument;

import com.mongodb.custombalancer.CustomBalancerConfig;
import com.mongodb.shardsync.ShardClient;

/**
 * Context object containing all state needed for balancing decisions.
 * Passed to each strategy to make migration decisions.
 */
public class BalancerContext {

    private final CustomBalancerConfig config;
    private final ShardClient shardClient;

    // Prefix groups by prefix value
    private Map<BsonDocument, PrefixGroup> prefixGroups;

    // Shard load metrics
    private Map<String, ShardLoadMetrics> shardMetrics;

    // Migration cooldown tracking
    private Map<BsonDocument, LocalDateTime> prefixLastMigrationTime;

    // Round information
    private int roundNumber;
    private LocalDateTime roundStartTime;

    public BalancerContext(CustomBalancerConfig config, ShardClient shardClient) {
        this.config = config;
        this.shardClient = shardClient;
        this.prefixGroups = new HashMap<>();
        this.shardMetrics = new HashMap<>();
        this.prefixLastMigrationTime = new HashMap<>();
        this.roundNumber = 0;
        this.roundStartTime = LocalDateTime.now();
    }

    public CustomBalancerConfig getConfig() {
        return config;
    }

    public ShardClient getShardClient() {
        return shardClient;
    }

    public Map<BsonDocument, PrefixGroup> getPrefixGroups() {
        return prefixGroups;
    }

    public void setPrefixGroups(Map<BsonDocument, PrefixGroup> prefixGroups) {
        this.prefixGroups = prefixGroups;
    }

    public PrefixGroup getPrefixGroup(BsonDocument prefixValue) {
        return prefixGroups.get(prefixValue);
    }

    public Collection<PrefixGroup> getAllPrefixGroups() {
        return prefixGroups.values();
    }

    public Map<String, ShardLoadMetrics> getShardMetrics() {
        return shardMetrics;
    }

    public void setShardMetrics(Map<String, ShardLoadMetrics> shardMetrics) {
        this.shardMetrics = shardMetrics;
    }

    public ShardLoadMetrics getShardMetrics(String shardId) {
        return shardMetrics.computeIfAbsent(shardId, ShardLoadMetrics::new);
    }

    public Collection<ShardLoadMetrics> getAllShardMetrics() {
        return shardMetrics.values();
    }

    public Map<BsonDocument, LocalDateTime> getPrefixLastMigrationTime() {
        return prefixLastMigrationTime;
    }

    public void recordMigration(BsonDocument prefixValue, LocalDateTime time) {
        prefixLastMigrationTime.put(prefixValue, time);
    }

    public boolean isPrefixInCooldown(BsonDocument prefixValue) {
        LocalDateTime lastMigration = prefixLastMigrationTime.get(prefixValue);
        if (lastMigration == null) {
            return false;
        }

        LocalDateTime now = LocalDateTime.now();
        long minutesSinceLastMigration = java.time.Duration.between(lastMigration, now).toMinutes();
        return minutesSinceLastMigration < config.getPrefixCooldownMinutes();
    }

    public int getRoundNumber() {
        return roundNumber;
    }

    public void incrementRoundNumber() {
        this.roundNumber++;
    }

    public LocalDateTime getRoundStartTime() {
        return roundStartTime;
    }

    public void setRoundStartTime(LocalDateTime roundStartTime) {
        this.roundStartTime = roundStartTime;
    }

    /**
     * Get all prefix groups on a specific shard.
     */
    public List<PrefixGroup> getPrefixGroupsOnShard(String shardId) {
        return prefixGroups.values().stream()
                .filter(pg -> pg.getShard().equals(shardId))
                .collect(Collectors.toList());
    }

    /**
     * Calculate ideal data size per shard (for size-based balancing).
     */
    public long getIdealDataSizePerShard() {
        long totalDataSize = shardMetrics.values().stream()
                .mapToLong(ShardLoadMetrics::getDataSizeBytes)
                .sum();

        int shardCount = shardMetrics.size();
        return shardCount > 0 ? totalDataSize / shardCount : 0;
    }

    /**
     * Calculate ideal operations per shard (for activity-based balancing).
     */
    public double getIdealOpsPerShard() {
        double totalOps = shardMetrics.values().stream()
                .mapToDouble(ShardLoadMetrics::getTotalOpsPerSecond)
                .sum();

        int shardCount = shardMetrics.size();
        return shardCount > 0 ? totalOps / shardCount : 0;
    }

    /**
     * Find the least loaded shard by data size.
     */
    public String getLeastLoadedShardByDataSize() {
        return shardMetrics.values().stream()
                .min((a, b) -> Long.compare(a.getDataSizeBytes(), b.getDataSizeBytes()))
                .map(ShardLoadMetrics::getShardId)
                .orElse(null);
    }

    /**
     * Find the least loaded shard by activity (ops/sec).
     */
    public String getLeastLoadedShardByActivity() {
        return shardMetrics.values().stream()
                .min((a, b) -> Double.compare(a.getTotalOpsPerSecond(), b.getTotalOpsPerSecond()))
                .map(ShardLoadMetrics::getShardId)
                .orElse(null);
    }

    /**
     * Find the most overloaded shard by data size.
     */
    public String getMostOverloadedShardByDataSize() {
        return shardMetrics.values().stream()
                .max((a, b) -> Long.compare(a.getDataSizeBytes(), b.getDataSizeBytes()))
                .map(ShardLoadMetrics::getShardId)
                .orElse(null);
    }

    /**
     * Find the most overloaded shard by activity (ops/sec).
     */
    public String getMostOverloadedShardByActivity() {
        return shardMetrics.values().stream()
                .max((a, b) -> Double.compare(a.getTotalOpsPerSecond(), b.getTotalOpsPerSecond()))
                .map(ShardLoadMetrics::getShardId)
                .orElse(null);
    }

    /**
     * Check if a shard is overloaded by data size based on configured threshold.
     */
    public boolean isShardOverloadedByDataSize(String shardId) {
        long ideal = getIdealDataSizePerShard();
        long actual = getShardMetrics(shardId).getDataSizeBytes();
        double thresholdMultiplier = 1.0 + (config.getImbalanceThresholdPercent() / 100.0);
        return actual > (ideal * thresholdMultiplier);
    }

    /**
     * Check if a shard is overloaded by activity based on configured threshold.
     */
    public boolean isShardOverloadedByActivity(String shardId) {
        double ideal = getIdealOpsPerShard();
        double actual = getShardMetrics(shardId).getTotalOpsPerSecond();
        double thresholdMultiplier = 1.0 + (config.getImbalanceThresholdPercent() / 100.0);
        return actual > (ideal * thresholdMultiplier);
    }
}