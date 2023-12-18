package com.mongodb.shardbalancer;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.WriteModel;

public class ChunkUpdateBuffer {
	
	protected static final Logger logger = LoggerFactory.getLogger(ChunkUpdateBuffer.class);
	
	Map<String, Map<Integer, Set<CountingMegachunk>>> chunkMap = new HashMap<>();
	
	Date startTime;
	Date endTime;
	
	private String shardId;
	
	
	public ChunkUpdateBuffer(String shardId) {
		this.shardId = shardId;
	}
	
	public void start() {
		startTime = new Date();
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
	
	public List<WriteModel<Document>> getWriteModels() {
		
		List<WriteModel<Document>> writeModels = new ArrayList<>();
		
		for (Map.Entry<String, Map<Integer, Set<CountingMegachunk>>> entry : chunkMap.entrySet()) {
			
			String ns = entry.getKey();
			Map<Integer, Set<CountingMegachunk>> innerMap = entry.getValue();
			
			for (Set<CountingMegachunk> chunkList : innerMap.values()) {
				
				Document checkpoint = new Document();
				
				int uberId = -1;
				long total = 0;
				int activeChunks = 0;
				
				if (chunkList.size() > 0) {
					uberId = chunkList.iterator().next().getUberId();
					checkpoint.append("uberId", uberId);
				}
				
				checkpoint.append("ns", ns);
				checkpoint.append("shard", shardId);
				checkpoint.append("startTime", startTime);
				endTime = new Date();
				checkpoint.append("endTime", endTime);
				
				List<Document> chunks = new ArrayList<>();
				//Map<String, Long> chunks = new LinkedHashMap<>();
				checkpoint.append("chunks", chunks);
				
				for (CountingMegachunk chunk : chunkList) {
					if (chunk.getUberId() != uberId) {
						logger.warn("uberId mismatch: {} {}", uberId, chunk.getUberId());
					}
					//chunks.put(chunk.getMin().getString("_id").getValue(), chunk.getSeenCount());
					
					
					BsonValue v = chunk.getMin().get("_id");
					//Object val = null;
					
//					if (v instanceof BsonString) {
//						val = ((BsonString)v).getValue();
//					}
					
					chunks.add(new Document("id", v).append("cnt", chunk.getSeenCount()));
					total += chunk.getSeenCount();
					activeChunks++;
				}
				checkpoint.append("total", total);
				checkpoint.append("activeChunks", activeChunks);
				
				WriteModel<Document> model = new InsertOneModel<Document>(checkpoint);
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

	
