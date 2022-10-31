package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

import com.mongodb.model.Namespace;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.diffutil.OplogTailingDiffTaskResult;
import com.mongodb.mongosync.ChunkCloneResult;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class DiffUtil {

	private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);


	private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private DiffConfiguration config;
    
    protected ThreadPoolExecutor executor = null;
	private BlockingQueue<Runnable> workQueue;
	List<Future<DiffResult>> diffResults;
	
	private Map<String, RawBsonDocument> sourceChunksCache;
	private Set<String> chunkCollSet;
	private long estimatedTotalDocs;
	
	
    
    public DiffUtil(DiffConfiguration config) {
    	this.config = config;
    	
    	sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
		destShardClient = new ShardClient("dest", config.getDestClusterUri());
		
		sourceShardClient.init();
		destShardClient.init();
		sourceShardClient.populateCollectionsMap();
		Pair<Map<String, RawBsonDocument>, Set<String>> chunkCachePair =
				sourceShardClient.loadChunksCachePlusCollections(config.getChunkQuery());
		sourceChunksCache = chunkCachePair.getLeft();
		chunkCollSet = chunkCachePair.getRight();
		estimatedTotalDocs = estimateCount(chunkCollSet);
		
		workQueue = new ArrayBlockingQueue<Runnable>(sourceChunksCache.size());
		diffResults = new ArrayList<>(sourceChunksCache.size());
		executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 30,
				TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
    }

	private long estimateCount(Set<String> collSet) {
		long sum = 0l;
		for (String s : collSet) {
			Namespace ns = new Namespace(s);
			Number cnt = sourceShardClient.getCollectionCount(ns.getDatabaseName(), ns.getCollectionName());
			sum += cnt.longValue();
		}
		return sum;
	}
    
    public void run() {
    	DiffSummary summary = new DiffSummary(sourceChunksCache.size(), estimatedTotalDocs);
    	for (RawBsonDocument chunk : sourceChunksCache.values()) {
    		
    		DiffTask task = new DiffTask(sourceShardClient, destShardClient, config, chunk, summary);
    		
    		diffResults.add(executor.submit(task));
    	}

		ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
		statusReporter.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				logger.info(summary.getSummary());
			}
		}, 0, 5, TimeUnit.SECONDS);

    	
    	executor.shutdown();
    	
    	for(Future<DiffResult> future : diffResults) {
    		try {
				logger.debug("result: {}", future.get());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
		statusReporter.shutdown();
		logger.info(summary.getSummary());
    }
	


}
