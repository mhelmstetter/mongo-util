package com.mongodb.shardbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

public class ChunkStats {
	
	protected static final Logger logger = LoggerFactory.getLogger(ChunkStats.class);
	
	private Map<String, List<ChunkStatsEntry>> chunkStatsMap = new HashMap<>();
	
	public void addEntry(ChunkStatsEntry entry) {
		List<ChunkStatsEntry> entries = chunkStatsMap.get(entry.getNamespace());
		if (entries == null) {
			entries = new ArrayList<>();
			chunkStatsMap.put(entry.getNamespace(), entries);
		}
		entries.add(entry);
	}
	
	public void updateTargetOpsPerShard(String ns, double deltaThresholdPercent, Set<String> shardsSet, int activeChunkThreshold) {
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
		if (entries == null || entries.isEmpty()) {
			logger.debug("no ChunkStatsEntry for ns: {}", ns);
			return;
		}
		long totalOps = 0;
		double totalOpsPerChunk = 0;
		for (ChunkStatsEntry entry : entries) {
			totalOps += entry.getTotalOps();
			totalOpsPerChunk += entry.getOpsPerChunk();
		}
		long targetOpsPerShard = totalOps / shardsSet.size();
		double avgOpsPerChunk = totalOpsPerChunk / shardsSet.size();
		
		boolean missingShardsFromSample = entries.size() != shardsSet.size();
		Set<String> seenShards = null;
		if (missingShardsFromSample) {
			seenShards = new HashSet<>();
		}
		
		
		logger.debug(String.format("ns: %s, numShards: %s, totalOps: %,d, targetOpsPerShard: %,d, avgOpsPerChunk: %,.2f", 
				ns, entries.size(), totalOps, targetOpsPerShard, avgOpsPerChunk));
		
		for (ChunkStatsEntry entry : entries) {
			 long delta = targetOpsPerShard - entry.getTotalOps();
			 double deltaPct = 0.0;
			 
			 if (targetOpsPerShard > 0) {
				 deltaPct = (100.0 * entry.getTotalOps()) / targetOpsPerShard;
			 }
			 
			 entry.setAboveThreshold((Math.abs(deltaPct - 100) >= deltaThresholdPercent || entry.getTotalOps() == 0) 
					 && entry.getActiveChunks() > activeChunkThreshold);
			 entry.setDeltaOps(delta);
			 logger.debug(String.format("shard: %s, activeChunks: %,d, totalOps: %,d, opsPerChunk: %,.2f, deltaPct: %.2f, aboveThreshold: %s", 
					 entry.getShard(), entry.getActiveChunks(), entry.getTotalOps(), 
					 entry.getOpsPerChunk(), deltaPct, entry.isAboveThreshold()));
			 
			 if (missingShardsFromSample) {
				 seenShards.add(entry.getShard());
			 }
		}
		
		if (missingShardsFromSample) {
			Set<String> missingShards = Sets.difference(shardsSet, seenShards);
			for (String shard : missingShards) {
				ChunkStatsEntry chunkStatsEntry = new ChunkStatsEntry(ns, shard, 0L, 0);
				entries.add(chunkStatsEntry);
			}
			logger.debug("There were {} shards with no activity this sample", missingShards.size());
		}
	}
	
	public List<ChunkStatsEntry> getEntries(String ns) {
		return chunkStatsMap.get(ns);
	}
	
	public String getHottestShard(String ns) {
		// results are sorted descending by totalOps, first shard is hottest
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
		if (entries == null || entries.isEmpty()) {
			logger.warn("could not get chunk stats for ns: {}", ns);
			return null;
		}
		return entries.get(0).getShard();
	}
	
	public String getColdestShard(String ns) {
		// results are sorted descending by totalOps, last shard is hottest
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
		return entries.get(entries.size()-1).getShard();
	}
	
	public int size() {
		return chunkStatsMap.size();
	}
	
	public boolean isEmpty() {
		return chunkStatsMap.isEmpty();
	}
	
	

}
