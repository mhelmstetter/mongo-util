package com.mongodb.custombalancer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.BaseConfiguration;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonValueWrapper;

public class CustomBalancerConfig extends BaseConfiguration {

    // Connection
    private ShardClient shardClient;

    // Shard Key Prefix Configuration
    private String shardKeyPrefix;
    private String[] shardKeyPrefixFields;

    // Strategy Configuration
    private String strategy = "activity";  // activity | dataSize | composite | whale
    private double compositeWeightActivity = 0.7;
    private double compositeWeightDataSize = 0.3;
    private double maxDataSizeDeviationPercent = 15.0;

    // Balancing Thresholds
    private double imbalanceThresholdPercent = 10.0;
    private int maxMigrationsPerRound = 5;
    private int roundIntervalSeconds = 300;
    private int prefixCooldownMinutes = 60;

    // Whale Detection
    private boolean whaleDetectionEnabled = true;
    private Map<String, Long> whaleMinDocuments;  // collection -> min doc count

    // Query Stats
    private int queryStatsIntervalSeconds = 300;

    // Safety
    private boolean dryRun = false;

    // Internal state tracking
    private Namespace migrationNamespace = new Namespace("mongoCustomBalancer", "migrations");
    private Namespace stateNamespace = new Namespace("mongoCustomBalancer", "balancerState");

    private MongoCollection<Document> migrationCollection;
    private MongoCollection<Document> stateCollection;

    // Chunk map cache
    private Map<String, NavigableMap<BsonValueWrapper, ?>> chunkMap;

    public String getShardKeyPrefix() {
        return shardKeyPrefix;
    }

    public void setShardKeyPrefix(String shardKeyPrefix) {
        this.shardKeyPrefix = shardKeyPrefix;
    }

    public String[] getShardKeyPrefixFields() {
        return shardKeyPrefixFields;
    }

    public void setShardKeyPrefixFields(String[] shardKeyPrefixFields) {
        this.shardKeyPrefixFields = shardKeyPrefixFields;
    }

    public void setShardKeyPrefixFields(String shardKeyPrefixFieldsStr) {
        if (shardKeyPrefixFieldsStr != null && !shardKeyPrefixFieldsStr.isEmpty()) {
            this.shardKeyPrefixFields = shardKeyPrefixFieldsStr.split(",");
        }
    }

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public double getCompositeWeightActivity() {
        return compositeWeightActivity;
    }

    public void setCompositeWeightActivity(double compositeWeightActivity) {
        this.compositeWeightActivity = compositeWeightActivity;
    }

    public double getCompositeWeightDataSize() {
        return compositeWeightDataSize;
    }

    public void setCompositeWeightDataSize(double compositeWeightDataSize) {
        this.compositeWeightDataSize = compositeWeightDataSize;
    }

    public double getMaxDataSizeDeviationPercent() {
        return maxDataSizeDeviationPercent;
    }

    public void setMaxDataSizeDeviationPercent(double maxDataSizeDeviationPercent) {
        this.maxDataSizeDeviationPercent = maxDataSizeDeviationPercent;
    }

    public double getImbalanceThresholdPercent() {
        return imbalanceThresholdPercent;
    }

    public void setImbalanceThresholdPercent(double imbalanceThresholdPercent) {
        this.imbalanceThresholdPercent = imbalanceThresholdPercent;
    }

    public int getMaxMigrationsPerRound() {
        return maxMigrationsPerRound;
    }

    public void setMaxMigrationsPerRound(int maxMigrationsPerRound) {
        this.maxMigrationsPerRound = maxMigrationsPerRound;
    }

    public int getRoundIntervalSeconds() {
        return roundIntervalSeconds;
    }

    public void setRoundIntervalSeconds(int roundIntervalSeconds) {
        this.roundIntervalSeconds = roundIntervalSeconds;
    }

    public int getPrefixCooldownMinutes() {
        return prefixCooldownMinutes;
    }

    public void setPrefixCooldownMinutes(int prefixCooldownMinutes) {
        this.prefixCooldownMinutes = prefixCooldownMinutes;
    }

    public boolean isWhaleDetectionEnabled() {
        return whaleDetectionEnabled;
    }

    public void setWhaleDetectionEnabled(boolean whaleDetectionEnabled) {
        this.whaleDetectionEnabled = whaleDetectionEnabled;
    }

    public Map<String, Long> getWhaleMinDocuments() {
        return whaleMinDocuments;
    }

    public void setWhaleMinDocuments(Map<String, Long> whaleMinDocuments) {
        this.whaleMinDocuments = whaleMinDocuments;
    }

    public int getQueryStatsIntervalSeconds() {
        return queryStatsIntervalSeconds;
    }

    public void setQueryStatsIntervalSeconds(int queryStatsIntervalSeconds) {
        this.queryStatsIntervalSeconds = queryStatsIntervalSeconds;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }

    public Namespace getMigrationNamespace() {
        return migrationNamespace;
    }

    public void setMigrationNamespace(Namespace migrationNamespace) {
        this.migrationNamespace = migrationNamespace;
    }

    public Namespace getStateNamespace() {
        return stateNamespace;
    }

    public void setStateNamespace(Namespace stateNamespace) {
        this.stateNamespace = stateNamespace;
    }

    public MongoCollection<Document> getMigrationCollection() {
        return migrationCollection;
    }

    public void setMigrationCollection(MongoCollection<Document> migrationCollection) {
        this.migrationCollection = migrationCollection;
    }

    public MongoCollection<Document> getStateCollection() {
        return stateCollection;
    }

    public void setStateCollection(MongoCollection<Document> stateCollection) {
        this.stateCollection = stateCollection;
    }

    public Map<String, NavigableMap<BsonValueWrapper, ?>> getChunkMap() {
        return chunkMap;
    }

    public void setChunkMap(Map<String, NavigableMap<BsonValueWrapper, ?>> chunkMap) {
        this.chunkMap = chunkMap;
    }

    public ShardClient getShardClient() {
        return shardClient;
    }

    public void setShardClient(ShardClient shardClient) {
        this.shardClient = shardClient;
    }

    public void setSourceShardClient(ShardClient shardClient) {
        this.shardClient = shardClient;
        super.setSourceShardClient(shardClient);
    }
}