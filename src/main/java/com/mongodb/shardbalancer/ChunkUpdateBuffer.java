package com.mongodb.shardbalancer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class ChunkUpdateBuffer {
	
	protected static final Logger logger = LoggerFactory.getLogger(ChunkUpdateBuffer.class);
	
	Map<String, Map<Integer, Set<CountingMegachunk>>> chunkMap = new HashMap<>();
	
	BsonDateTime startTime;
	BsonDateTime endTime;
	
	private String shardId;
	private BalancerConfig config;
	
	
	public ChunkUpdateBuffer(String shardId, BalancerConfig config) {
		this.shardId = shardId;
		this.config = config;
	}
	
	public void start() {
		startTime = new BsonDateTime(System.currentTimeMillis());
	}
	
	
	public void add(CountingMegachunk m) {
		
		Map<Integer, Set<CountingMegachunk>> innerMap = chunkMap.get(m.getNs());
		if (innerMap == null) {
			innerMap = new HashMap<>();
			chunkMap.put(m.getNs(), innerMap);
		}
		
		Set<CountingMegachunk> chunks = innerMap.get(m.getUberId());
		if (chunks == null) {
			chunks = new HashSet<>();
			innerMap.put(m.getUberId(), chunks);
		}
		chunks.add(m);
	}
	
	public List<WriteModel<BsonDocument>> getWriteModels() {
		
		List<WriteModel<BsonDocument>> writeModels = new ArrayList<>();
		
		for (Map.Entry<String, Map<Integer, Set<CountingMegachunk>>> entry : chunkMap.entrySet()) {
			
			String ns = entry.getKey();
			Map<Integer, Set<CountingMegachunk>> innerMap = entry.getValue();
			
			for (Set<CountingMegachunk> chunkList : innerMap.values()) {
				
				BsonDocument checkpoint = new BsonDocument();
				
				int uberId = -1;
				long total = 0;
				int activeChunks = 0;
				
				if (chunkList.size() > 0) {
					uberId = chunkList.iterator().next().getUberId();
					checkpoint.append("uberId", new BsonInt64(uberId));
				}
				
				checkpoint.append("analysisId", config.getAnalysisId());
				checkpoint.append("ns", new BsonString(ns));
				checkpoint.append("shard", new BsonString(shardId));
				checkpoint.append("startTime", startTime);
				endTime = new BsonDateTime(System.currentTimeMillis());
				checkpoint.append("endTime", endTime);
				
				
				BsonArray chunks = new BsonArray();
				//Map<String, Long> chunks = new LinkedHashMap<>();
				checkpoint.append("chunks", chunks);
				
				for (CountingMegachunk chunk : chunkList) {
					if (chunk.getUberId() != uberId) {
						logger.warn("uberId mismatch: {} {}", uberId, chunk.getUberId());
					}
					//chunks.put(chunk.getMin().getString("_id").getValue(), chunk.getSeenCount());
					
					
					BsonValue v = chunk.getMin().get("_id");
					
					if (v == null) {
						logger.warn("chunk min does not have _id: {}", chunk);
						continue;
					}
					
					chunks.add(new BsonDocument("id", v).append("cnt", new BsonInt64(chunk.getSeenCount())));
					total += chunk.getSeenCount();
					activeChunks++;
				}
				checkpoint.append("total", new BsonInt64(total));
				checkpoint.append("activeChunks", new BsonInt64(activeChunks));
				
				WriteModel<BsonDocument> model = new InsertOneModel<BsonDocument>(checkpoint);
				writeModels.add(model);
			}
		}
		return writeModels;
		
		
	}

	public void clear() {
		
		startTime = endTime;
		
		for (Map<Integer, Set<CountingMegachunk>> inner : chunkMap.values()) {
			for (Set<CountingMegachunk> chunkList : inner.values()) {
				for (CountingMegachunk chunk : chunkList) {
					chunk.setSeenCount(0);
				}
			}
		}
		chunkMap.clear();
	}
 	

}

	
