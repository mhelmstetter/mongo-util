package com.mongodb.shardbalancer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.Document;
import org.bson.RawBsonDocument;

import com.mongodb.client.MongoCollection;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.BaseConfiguration;
import com.mongodb.util.bson.BsonValueWrapper;

public class BalancerConfig extends BaseConfiguration {
	
	private int analyzerSleepIntervalMinutes;
	
	private int moveCountBackoffThreshold;
	
	private int activeChunkThreshold;
	
	// used for forcing a certain number of chunks to move per namespace
	private int chunksToMove = 0;
	
	private Namespace balancerStateNamespace = new Namespace("mongoCustomBalancerStats", "balancerState");
	
	private Namespace statsNamespace = new Namespace("mongoCustomBalancerStats", "chunkStats");
	
	private Namespace balancerRoundNamespace = new Namespace("mongoCustomBalancerStats", "balancerRound");
	
	private MongoCollection<BsonDocument> statsCollection;
	
	private MongoCollection<Document> balancerRoundCollection;
	
	private MongoCollection<Document> balancerStateCollection;
	
	protected Set<String> sourceShards;
	
	private boolean dryRun;
	
	private double deltaThresholdPercent;
	
	// chunk stats
	Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap;
	
	// all chunks
	Map<String, RawBsonDocument> sourceChunksCache = new LinkedHashMap<>();
	
	AtomicBoolean runAnalyzer = new AtomicBoolean(false);
	
	private BsonObjectId analysisId;

	public Namespace getStatsNamespace() {
		return statsNamespace;
	}

	public void setStatsNamespace(Namespace statsNamespace) {
		this.statsNamespace = statsNamespace;
	}

	public MongoCollection<BsonDocument> getStatsCollection() {
		return statsCollection;
	}

	public void setStatsCollection(MongoCollection<BsonDocument> statsCollection) {
		this.statsCollection = statsCollection;
	}
	
	public void setSourceShards(String[] shards) {
		this.sourceShards = new HashSet<>();
		sourceShards.addAll(Arrays.asList(shards));
	}

	public Set<String> getSourceShards() {
		return sourceShards;
	}

	public int getAnalyzerSleepIntervalMinutes() {
		return analyzerSleepIntervalMinutes;
	}
	
	public int getAnalyzerSleepIntervalMillis() {
		return analyzerSleepIntervalMinutes  * 60 * 1000;
	}
	

	public void setAnalyzerSleepIntervalMinutes(int analyzerSleepIntervalMinutes) {
		this.analyzerSleepIntervalMinutes = analyzerSleepIntervalMinutes;
	}

	public Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> getChunkMap() {
		return chunkMap;
	}

	public void setChunkMap(Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap) {
		this.chunkMap = chunkMap;
	}
	
	public void setRunAnalyzer(boolean running) {
		runAnalyzer.set(running);
	}
	
	public boolean runAnalyzer() {
		return runAnalyzer.get();
	}

	public BsonObjectId getAnalysisId() {
		return analysisId;
	}

	public void setAnalysisId(BsonObjectId analysisId) {
		this.analysisId = analysisId;
	}

	public MongoCollection<Document> getBalancerRoundCollection() {
		return balancerRoundCollection;
	}

	public void setBalancerRoundCollection(MongoCollection<Document> balancerRoundCollection) {
		this.balancerRoundCollection = balancerRoundCollection;
	}

	public Namespace getBalancerRoundNamespace() {
		return balancerRoundNamespace;
	}

	public void setBalancerRoundNamespace(Namespace balancerRoundNamespace) {
		this.balancerRoundNamespace = balancerRoundNamespace;
	}

	public MongoCollection<Document> getBalancerStateCollection() {
		return balancerStateCollection;
	}

	public void setBalancerStateCollection(MongoCollection<Document> balancerStateCollection) {
		this.balancerStateCollection = balancerStateCollection;
	}

	public Namespace getBalancerStateNamespace() {
		return balancerStateNamespace;
	}

	public void setBalancerStateNamespace(Namespace balancerStateNamespace) {
		this.balancerStateNamespace = balancerStateNamespace;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public double getDeltaThresholdPercent() {
		return deltaThresholdPercent;
	}

	public void setDeltaThresholdPercent(double deltaThresholdPercent) {
		this.deltaThresholdPercent = deltaThresholdPercent;
	}

	public int getMoveCountBackoffThreshold() {
		return moveCountBackoffThreshold;
	}

	public void setMoveCountBackoffThreshold(int moveCountBackoffThreshold) {
		this.moveCountBackoffThreshold = moveCountBackoffThreshold;
	}

	public int getActiveChunkThreshold() {
		return activeChunkThreshold;
	}

	public void setActiveChunkThreshold(int activeChunkThreshold) {
		this.activeChunkThreshold = activeChunkThreshold;
	}

	public int getChunksToMove() {
		return chunksToMove;
	}

	public void setChunksToMove(int chunksToMove) {
		this.chunksToMove = chunksToMove;
	}

	public Map<String, RawBsonDocument> getSourceChunksCache() {
		return sourceChunksCache;
	}
	
	

}
