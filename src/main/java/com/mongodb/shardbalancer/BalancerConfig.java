package com.mongodb.shardbalancer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import org.bson.Document;

import com.mongodb.client.MongoCollection;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.BaseConfiguration;

public class BalancerConfig extends BaseConfiguration {
	
	private int checkpointIntervalMinutes;
	
	private int analyzerSleepIntervalMinutes;
	
	private int balancerChunkBatchSize;
	
	private Namespace statsNamespace = new Namespace("mongoCustomBalancerStats", "chunkStats");
	
	private MongoCollection<Document> statsCollection;
	
	protected Set<String> sourceShards;
	
	Map<String, NavigableMap<String, CountingMegachunk>> chunkMap;

	public int getCheckpointIntervalMinutes() {
		return checkpointIntervalMinutes;
	}
	
	public int getCheckpointIntervalMillis() {
		return checkpointIntervalMinutes * 60 * 1000;
	}
	

	public void setCheckpointIntervalMinutes(int checkpointIntervalMinutes) {
		this.checkpointIntervalMinutes = checkpointIntervalMinutes;
	}

	public Namespace getStatsNamespace() {
		return statsNamespace;
	}

	public void setStatsNamespace(Namespace statsNamespace) {
		this.statsNamespace = statsNamespace;
	}

	public MongoCollection<Document> getStatsCollection() {
		return statsCollection;
	}

	public void setStatsCollection(MongoCollection<Document> statsCollection) {
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

	public int getBalancerChunkBatchSize() {
		return balancerChunkBatchSize;
	}

	public void setBalancerChunkBatchSize(int balancerChunkBatchSize) {
		this.balancerChunkBatchSize = balancerChunkBatchSize;
	}

	public Map<String, NavigableMap<String, CountingMegachunk>> getChunkMap() {
		return chunkMap;
	}

	public void setChunkMap(Map<String, NavigableMap<String, CountingMegachunk>> chunkMap) {
		this.chunkMap = chunkMap;
	}
	
	

}
