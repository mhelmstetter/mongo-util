package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	
    
    public DiffUtil(DiffConfiguration config) {
    	this.config = config;
    	
    	sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
		destShardClient = new ShardClient("dest", config.getDestClusterUri());
		
		sourceShardClient.init();
		destShardClient.init();
		
		sourceChunksCache = sourceShardClient.loadChunksCache(config.getChunkQuery());
		
		workQueue = new ArrayBlockingQueue<Runnable>(sourceChunksCache.size());
		diffResults = new ArrayList<>(sourceChunksCache.size());
		executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
    }
    
    public void run() {
    	
    	for (RawBsonDocument chunk : sourceChunksCache.values()) {
    		
    		DiffTask task = new DiffTask(sourceShardClient, destShardClient, config, chunk);
    		
    		diffResults.add(executor.submit(task));
    	}
    }
	


}
