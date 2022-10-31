package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	private ScheduledExecutorService statusReporter;
	
	private Map<String, RawBsonDocument> sourceChunksCache;
	
	private int chunksComplete = 0;
	private int chunksFailed = 0;
	private int totalChunks;
	private int totalDocumentsFailed;
	private int onlyOnSource = 0;
	private int onlyOnDest = 0;
	
    
    public DiffUtil(DiffConfiguration config) {
    	this.config = config;
    	
    	sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
		destShardClient = new ShardClient("dest", config.getDestClusterUri());
		
		sourceShardClient.init();
		destShardClient.init();
		sourceShardClient.populateCollectionsMap();
		sourceChunksCache = sourceShardClient.loadChunksCache(config.getChunkQuery());
		
		totalChunks = sourceChunksCache.size();
		workQueue = new ArrayBlockingQueue<Runnable>(totalChunks);
		diffResults = new ArrayList<>(totalChunks);
		executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
		
		statusReporter = Executors.newSingleThreadScheduledExecutor();
		statusReporter.scheduleAtFixedRate(new Runnable() {
		  @Override
		  public void run() {
			  reportStatus();
		  }
		}, 0, 5, TimeUnit.SECONDS);
    }
    
    private void reportStatus() {
    	double complete = chunksComplete / totalChunks;
		logger.debug(String.format("%4.1f: %s/%s chunks complete, chunks failed: %s, docs failed: %s, onlyOnSource: %s, onlyOnDest: %s", 
				complete, chunksComplete, totalChunks, chunksFailed, totalDocumentsFailed, onlyOnSource, onlyOnDest));
    }
    
    public void run() {
    	
    	for (RawBsonDocument chunk : sourceChunksCache.values()) {
    		DiffTask task = new DiffTask(sourceShardClient, destShardClient, config, chunk);
    		diffResults.add(executor.submit(task));
    	}
    	
    	executor.shutdown();
    	
    	for(Future<DiffResult> future : diffResults) {
    		try {
    			DiffResult result = future.get();
    			int failures = result.getFailureCount();
    			if (failures > 0) {
    				chunksFailed++;
    			}
    			totalDocumentsFailed += failures;
    			chunksComplete++;
    			onlyOnSource += result.onlyOnSource;
    			onlyOnDest += result.onlyOnDest;
    			
				//logger.debug("result: {}", );
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExecutionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	statusReporter.shutdown();
    	reportStatus();
    }
	


}
