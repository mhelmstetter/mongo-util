package com.mongodb.shard;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.bson.RawBsonDocument;

import com.google.common.collect.Sets;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

public class V3BalancingStrategy extends BalancingStrategy {
	
	private Set<String> sourceShards = new HashSet<>();
	
	private Set<String> namespaces = new HashSet<>();

	public V3BalancingStrategy(String uri) {
		super(uri);
	}

	@Override
	public void balance() {
		
		
		
		for (String ns : namespaces) {
			
			Map<String, Integer> shardChunkCountMap = new HashMap<>();
			
			//Set<String> destShards = new HashSet<>();
			
			List<Document> chunksCountResults = new ArrayList<>();
			chunks.aggregate(Arrays.asList(match(Filters.eq("ns", ns)), group("$shard", Accumulators.sum("count", 1)),
					sort(Sorts.descending("count")))).into(chunksCountResults);
			
			int totalChunks = 0;
			int chunksToMoveCount = 0;
			int destShardChunkCount = 0;
			for (Document chunksCount : chunksCountResults) {
				String shard = chunksCount.getString("_id");
				Integer chunkCount = chunksCount.getInteger("count");
				
				if (sourceShards.contains(shard)) {
					chunksToMoveCount += chunkCount;
				} else {
					destShardChunkCount += chunkCount;
				}
				
				totalChunks += chunkCount;
				shardChunkCountMap.put(shard, chunkCount);
				logger.debug("{}: {} - chunkCount: {}", ns, shard, chunkCount);
			}
			int totalShardCount = shardClient.getShardsMap().size();
			
			Set<String> destShards = Sets.difference(shardClient.getShardsMap().keySet(), sourceShards);
			
			int destShardCount = totalShardCount - sourceShards.size();
			
			logger.debug("{}: totalShardCount: {}, destShardCount: {}, totalChunkCount: {}", 
					ns, totalShardCount, destShardCount, totalChunks);
			
			int targetChunkCount = totalChunks / totalShardCount;
			
			logger.debug("{}: targetChunkCount: {}", ns, targetChunkCount);
			
			

			List<RawBsonDocument> sourceChunks = new ArrayList<>();
			chunks.find(Filters.and(Filters.eq("ns", ns), Filters.in("shard", sourceShards)), RawBsonDocument.class)
					.into(sourceChunks);
			Collections.shuffle(sourceChunks);
			Iterator<RawBsonDocument> chunksToMove = sourceChunks.iterator();
			
			int totalMoved = 0;
			for (String shard : destShards) {
				int currentCount = shardChunkCountMap.containsKey(shard) ? shardChunkCountMap.get(shard) : 0;
				int moveCount = targetChunkCount - currentCount;
				logger.debug("{}: {}, currentCount: {}, moveCount: {}", ns, shard, currentCount, moveCount);
				
				if (! dryRun) {
					for (int i = 0; i < moveCount; i++) {
						
						if (chunksToMove.hasNext()) {
							RawBsonDocument chunk = chunksToMove.next();
							shardClient.moveChunk(chunk, shard, false, true, true);
							String sourceShard = chunk.getString("shard").getValue();
							totalMoved++;
							logger.debug("{}: moved chunk ({}->{}), totalMoved: {}", ns, sourceShard, shard, totalMoved);
						}
						sleep();
					}
				}
				
			}
		}
		
	}
	
	private void sleep() {
		try {
			Thread.sleep(1);
		} catch (InterruptedException e) {
		}
	}
 
	public void setSourceShards(String[] sourceShards) {
		this.sourceShards.addAll(Arrays.asList(sourceShards));
	}

	public void setNamespaces(String[] namespaces) {
		this.namespaces.addAll(Arrays.asList(namespaces));
	}

}
