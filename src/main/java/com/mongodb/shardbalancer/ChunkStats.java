package com.mongodb.shardbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	public void updateTargetOpsPerShard(String ns) {
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
		long targetOpsPerShard = totalOps / entries.size();
		double avgOpsPerChunk = totalOpsPerChunk / entries.size();
		
		logger.debug("ns: {}, totalOps: {}, targetOpsPerShard: {}, avgOpsPerChunk: {}", ns, totalOps, targetOpsPerShard, avgOpsPerChunk);
		
		for (ChunkStatsEntry entry : entries) {
			 long delta = targetOpsPerShard - entry.getTotalOps();
			 int chunks = (int)(delta / entry.getOpsPerChunk());
			 entry.setChunksToMove(chunks);
			 logger.debug("shard: {}, activeChunks: {}, totalOps: {}, opsPerChunk: {}, delta: {}, chunksToMove: {}", 
					 entry.getShard(), entry.getActiveChunks(), entry.getTotalOps(), 
					 entry.getOpsPerChunk(), delta, chunks);
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
