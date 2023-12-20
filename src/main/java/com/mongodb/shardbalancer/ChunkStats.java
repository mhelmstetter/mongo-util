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
	
	public void getTargetOpsPerShard(String ns) {
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
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
			 double chunks = delta/ entry.getOpsPerChunk();
			 logger.debug("shard: {}, activeChunks: {}, totalOps: {}, opsPerChunk: {}, delta: {}, chunksToMove: {}", 
					 entry.getShard(), entry.getActiveChunks(), entry.getTotalOps(), 
					 entry.getOpsPerChunk(), delta, chunks);
		}
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
	
//    public List<String> getHottestColdestPairs(String ns) {
//        List<String> pairs = new ArrayList<>();
//
//        List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
//        if (entries == null || entries.isEmpty()) {
//            return pairs; // No entries to process
//        }
//
//        // Sort entries by totalOps in descending order
//        Collections.sort(entries, Collections.reverseOrder());
//
//        int size = entries.size();
//
//        for (int i = 0; i < size / 2; i++) {
//            // Pair hottest with coldest, 2nd hottest with 2nd coldest, and so on
//            String hottestShard = entries.get(i).getShard();
//            String coldestShard = entries.get(size - 1 - i).getShard();
//
//            pairs.add(hottestShard);
//            pairs.add(coldestShard);
//        }
//
//        return pairs;
//    }
	
	public String getHighestPriorityNamespace() {
		if (chunkStatsMap.isEmpty()) {
			logger.debug("getHighestPriorityNamespace() chunkStatsMap is empty");
			return null;
		}
		logger.debug("getHighestPriorityNamespace() keySet size: {}" + chunkStatsMap.keySet().size());
		// TODO handle multiple NS
		return chunkStatsMap.keySet().iterator().next();
	}
	
	public boolean isEmpty() {
		return chunkStatsMap.isEmpty();
	}
	
	

}
