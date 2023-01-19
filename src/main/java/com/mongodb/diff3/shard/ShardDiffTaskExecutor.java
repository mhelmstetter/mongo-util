package com.mongodb.diff3.shard;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.DiffSummaryClient;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Namespace;
import com.mongodb.util.BlockWhenQueueFull;

public class ShardDiffTaskExecutor {

	private static final Logger logger = LoggerFactory.getLogger(ShardDiffUtil.class);

	private final String sourceShardName;
	private final String destShardName;
	private final DiffConfiguration config;
	private final DiffSummaryClient diffSummaryClient;
	private final Map<String, RawBsonDocument> chunkMap;
	private final int numThreads;
	private final DiffSummary summary;
	
	private Queue<RetryTask> retryQueue;

	private ExecutorCompletionService<DiffResult> completionService;
	private ThreadPoolExecutor threadPoolExecutor;
	
	private AtomicBoolean complete = new AtomicBoolean(false);
	
	private int submitCount = 0;
	private int count = 0;

	public ShardDiffTaskExecutor(DiffConfiguration config, Map<String, RawBsonDocument> chunkMap,
			String sourceShardName, String destShardName, int numThreads, DiffSummary summary) {
		this.config = config;
		this.diffSummaryClient = config.getDiffSummaryClient();
		this.chunkMap = chunkMap;
		this.sourceShardName = sourceShardName;
		this.destShardName = destShardName;
		this.numThreads = numThreads;
		this.summary = summary;
	}

	public void initializeTasks() {
		retryQueue = new DelayQueue<>();
		BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(chunkMap.size());
		ThreadFactory initialTaskPoolThreadFactory = new ThreadFactoryBuilder()
				.setNameFormat("WorkerPool-" + sourceShardName + "-%d").build();
		threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, 30, TimeUnit.SECONDS,
				workQueue, new BlockWhenQueueFull());
		threadPoolExecutor.setThreadFactory(initialTaskPoolThreadFactory);
		// initialTaskPoolMap.put(shard, initialTaskPool);

		completionService = new ExecutorCompletionService<>(threadPoolExecutor);

		for (RawBsonDocument chunk : chunkMap.values()) {
			String nsStr = chunk.get("ns").asString().getValue();
			Namespace ns = new Namespace(nsStr);
			boolean complete = diffSummaryClient.updateChunkCompletion(ns, chunk, summary);
			if (complete) {
				logger.debug("Skipping chunk, already complete");
			} else {
				ShardDiffTask task = new ShardDiffTask(config, chunk, ns, sourceShardName, destShardName, retryQueue,
						summary);
				// List<Future<DiffResult>> initialTaskPoolFutures =
				// initialTaskPoolFutureMap.get(srcShard);
				// ThreadPoolExecutor initialTaskPool = initialTaskPoolMap.get(srcShard);
				completionService.submit(task);
				submitCount++;
				// initialTaskPoolFutures.add(initialTaskPool.submit(task));
			}
		}
	}

	public void run() {
		ScheduledExecutorService initialTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
		initialTaskPoolCollector.scheduleAtFixedRate(new Runnable() {
		

			@Override
			public void run() {
				
				while (count < submitCount) {
					logger.debug("{} [InitialTaskPoolCollector] loop: {}, tasks: {}", sourceShardName, count, threadPoolExecutor.getQueue().size());

					try {

						//logger.debug("**** {} starting take/get: {}", sourceShardName, count);
						DiffResult result = completionService.take().get();
						count++;
						//logger.debug("**** {} finished take/get: {}", sourceShardName, count);
						int failures = result.getFailedKeys().size();
						if (failures > 0) {
							logger.debug("[InitialTaskPoolCollector] will retry {} failed ids for ({})",
									result.getFailedKeys().size(), result.getChunkDef().unitString());
						} else {
							logger.trace(
									"[InitialTaskPoolCollector] got result for ({}): "
											+ "{} matches, {} failures, {} bytes",
									result.getChunkDef().unitString(), result.getMatches(), result.getFailedKeys().size(),
									result.getBytesProcessed());
						}
						summary.updateInitTask(result);

					} catch (InterruptedException e) {
						logger.error("[InitialTaskPoolCollector] Diff task was interrupted", e);
					} catch (Exception e) {
						logger.error("[InitialTaskPoolCollector] Diff task threw an exception", e);
					}
				}

				logger.debug("Starting pool shutdown for shard {}", sourceShardName);
				threadPoolExecutor.shutdown();
				logger.debug("Pool shutdown complete for shard {}", sourceShardName);				
				initialTaskPoolCollector.shutdown();
				logger.debug("*** Collector shutdown complete for shard {}", sourceShardName);
				complete.set(true);
			}

		}, 0, 1, TimeUnit.SECONDS);
	}
	
	public boolean isComplete() {
		return complete.get();
	}

}
