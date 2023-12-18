package com.mongodb.shardbalancer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardClient;

public class TailingOplogAnalyzer {

	protected static final Logger logger = LoggerFactory.getLogger(TailingOplogAnalyzer.class);

	private ExecutorService executor;

	private BalancerConfig balancerConfig;
	private ShardClient sourceShardClient;

	public TailingOplogAnalyzer(BalancerConfig balancerConfig) {
		this.balancerConfig = balancerConfig;
		this.sourceShardClient = balancerConfig.getSourceShardClient();
	}

	

	public void start() {
        
		int poolSize = sourceShardClient.getShardsMap().size();
		executor = Executors.newFixedThreadPool(poolSize);
		for (String sourceShardId : sourceShardClient.getShardsMap().keySet()) {
			TailingOplogAnalyzerWorker worker = new TailingOplogAnalyzerWorker(sourceShardId, balancerConfig); 
        	executor.execute(worker);
		}
	}
	

	protected void stop() throws InterruptedException {
		executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(10000);
        }
        logger.debug("analyzer complete");
	}

}
