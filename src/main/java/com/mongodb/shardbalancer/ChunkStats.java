package com.mongodb.shardbalancer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ChunkStats {
	
	private Map<String, List<ChunkStatsEntry>> chunkStatsMap = new HashMap<>();
	
	//private Map<String, List<ChunkStatsEntry>> shardStatsMap = new HashMap<>();
	
	public void addEntry(ChunkStatsEntry entry) {
		List<ChunkStatsEntry> entries = chunkStatsMap.get(entry.getNamespace());
		if (entries == null) {
			entries = new ArrayList<>();
			chunkStatsMap.put(entry.getNamespace(), entries);
		}
		entries.add(entry);
		
//		List<ChunkStatsEntry> shardStats = shardStatsMap.get(entry.getShard());
//		if (shardStats == null) {
//			shardStats = new ArrayList<>();
//			shardStatsMap.put(entry.getShard(), shardStats);
//		}
	}
	
	public String getHottestShard(String ns) {
		// results are sorted descending by totalOps, first shard is hottest
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
		return entries.get(0).getShard();
	}
	
	public String getColdestShard(String ns) {
		// results are sorted descending by totalOps, last shard is hottest
		List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
		return entries.get(entries.size()-1).getShard();
	}
	
    public List<String> getHottestColdestPairs(String ns) {
        List<String> pairs = new ArrayList<>();

        List<ChunkStatsEntry> entries = chunkStatsMap.get(ns);
        if (entries == null || entries.isEmpty()) {
            return pairs; // No entries to process
        }

        // Sort entries by totalOps in descending order
        Collections.sort(entries, Collections.reverseOrder());

        int size = entries.size();

        for (int i = 0; i < size / 2; i++) {
            // Pair hottest with coldest, 2nd hottest with 2nd coldest, and so on
            String hottestShard = entries.get(i).getShard();
            String coldestShard = entries.get(size - 1 - i).getShard();

            pairs.add(hottestShard);
            pairs.add(coldestShard);
        }

        return pairs;
    }
	
	public String getHighestPriorityNamespace() {
		if (chunkStatsMap.isEmpty()) {
			return null;
		}
		// TODO handle multiple NS
		return chunkStatsMap.keySet().iterator().next();
	}
	
	public boolean isEmpty() {
		return chunkStatsMap.isEmpty();
	}
	
	

}
