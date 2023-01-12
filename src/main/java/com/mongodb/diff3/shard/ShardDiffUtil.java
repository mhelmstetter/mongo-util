package com.mongodb.diff3.shard;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.DiffSummary.DiffStatus;
import com.mongodb.diff3.DiffSummaryClient;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class ShardDiffUtil {

	private static final Logger logger = LoggerFactory.getLogger(ShardDiffUtil.class);
	private final List<String> destShardNames;
	private final List<String> srcShardNames;

	private final ShardClient sourceShardClient;
	private final ShardClient destShardClient;
	private final DiffSummaryClient diffSummaryClient;

	private final DiffConfiguration config;

	protected Map<String, ThreadPoolExecutor> initialTaskPoolMap = new HashMap<>();
	private ExecutorService retryTaskPool;
	// Map<String, List<Future<DiffResult>>> initialTaskPoolFutureMap = new
	// HashMap<>();

	private Map<String, ShardDiffTaskExecutor> shardDiffTaskExecutors = new HashMap<>();

	private Map<String, Map<String, RawBsonDocument>> sourceChunksCacheMap;
	private final long estimatedTotalDocs;
	private final long totalSize;
	private final int numUnshardedCollections;
	private Queue<RetryTask> retryQueue;
	private int totalInitialTasks;

	private int alreadyCompletedCount = 0;

	private ChunkManager chunkManager;

	private DiffSummary summary;

	public ShardDiffUtil(DiffConfiguration config) {

		this.config = config;

		chunkManager = new ChunkManager(config);
		chunkManager.initalize();

		this.sourceShardClient = config.getSourceShardClient();
		this.destShardClient = config.getDestShardClient();
		this.diffSummaryClient = config.getDiffSummaryClient();

		Set<String> includeNs = config.getIncludeNamespaces().stream().map(Namespace::getNamespace)
				.collect(Collectors.toSet());
		sourceShardClient.populateCollectionsMap(includeNs);
		DatabaseCatalog catalog = sourceShardClient.getDatabaseCatalog(config.getIncludeNamespaces());

		long[] sizeAndCount = catalog.getTotalSizeAndCount();
		totalSize = sizeAndCount[0];
		estimatedTotalDocs = sizeAndCount[1];

		Set<String> shardedColls = catalog.getShardedCollections().stream().map(c -> c.getNamespace().getNamespace())
				.collect(Collectors.toSet());
		Set<String> unshardedColls = catalog.getUnshardedCollections().stream()
				.map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());
		numUnshardedCollections = unshardedColls.size();
		totalInitialTasks += numUnshardedCollections;

		logger.info("[Main] shardedColls:[" + String.join(", ", shardedColls) + "]");
		logger.info("[Main] unshardedColls:[" + String.join(", ", unshardedColls) + "]");

		summary = new DiffSummary(estimatedTotalDocs, totalSize, diffSummaryClient);

		sourceShardClient.populateShardMongoClients();
		destShardClient.populateShardMongoClients();

		srcShardNames = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
		destShardNames = new ArrayList<>(destShardClient.getShardsMap().keySet());

	}

	private void loadChunks() {

		sourceChunksCacheMap = new HashMap<>();

		BsonDocument chunkQuery = chunkManager.getChunkQuery();
		Map<String, RawBsonDocument> sourceCache = sourceShardClient.loadChunksCache(chunkQuery);
		DiffSummaryClient diffSummaryClient = config.getDiffSummaryClient();

		BsonDocument completedChunksQuery = chunkQuery.clone();
		completedChunksQuery.append("status", new BsonString(DiffStatus.SUCCEEDED.toString()));
		Map<String, RawBsonDocument> completedChunks = diffSummaryClient.loadChunksCache(completedChunksQuery);

		int todoCount = 0;
		for (Map.Entry<String, RawBsonDocument> entry : sourceCache.entrySet()) {
			RawBsonDocument chunk = entry.getValue();
			String chunkId = entry.getKey();

			if (completedChunks.containsKey(chunkId)) {

				RawBsonDocument completedChunk = completedChunks.get(chunkId);

				int srcOnlyCount = completedChunk.getArray("srcOnly").size();
				int destOnlyCount = completedChunk.getArray("destOnly").size();
				int mismatches = completedChunk.getArray("mismatches").size();
				long bytesProcessed = completedChunk.getInt64("bytesProcessed").getValue();
				long matches = completedChunk.getInt64("matches").getValue();

				long totalProcessedDocs = matches + mismatches + srcOnlyCount + destOnlyCount;
				// (cr.getMatches().longValue() + numMismatches + numSourceOnly + numDestOnly);

				summary.getTotalSourceOnly().add(srcOnlyCount);
				summary.getTotalDestOnly().add(destOnlyCount);
				summary.getTotalFailedDocs().add(mismatches);
				summary.getTotalProcessedDocs().add(totalProcessedDocs);
				summary.getTotalProcessedSize().add(bytesProcessed);
				summary.getTotalProcessedChunks().increment();

				alreadyCompletedCount++;
				continue;
			}

			String shard = ShardClient.getShardFromChunk(chunk);

			if (!sourceChunksCacheMap.containsKey(shard)) {
				sourceChunksCacheMap.put(shard, new HashMap<>());
			}
			Map<String, RawBsonDocument> shardChunkCache = sourceChunksCacheMap.get(shard);
			shardChunkCache.put(chunkId, chunk);
			todoCount++;
		}
		logger.debug("ShardDiffUtil loaded chunk cache, completedCount: {}, todoCount: {}", alreadyCompletedCount,
				todoCount);
	}

	private int getTotalChunks() {
		AtomicInteger sum = new AtomicInteger();
		sourceChunksCacheMap.forEach((k, v) -> sum.addAndGet(v.size()));
		return sum.get() + numUnshardedCollections;
	}

	private void initializeTasks() {
		for (String sourceShardName : srcShardNames) {
			String destShardName = chunkManager.getShardMapping(sourceShardName);

			int numThreads = config.getThreads() / srcShardNames.size();
			Map<String, RawBsonDocument> chunkMap = sourceChunksCacheMap.get(sourceShardName);
			if (chunkMap == null) {
				logger.debug("No chunks for shard {}, skipping execution", sourceShardName);
				continue;
			}

			totalInitialTasks += chunkMap.size();

			ShardDiffTaskExecutor executor = new ShardDiffTaskExecutor(config, chunkMap, sourceShardName, destShardName,
					numThreads, summary);
			shardDiffTaskExecutors.put(sourceShardName, executor);
			executor.initializeTasks();
			executor.run();
		}

		logger.info("[Main] found {} initial tasks", totalInitialTasks);
	}

	private boolean shardDiffTaskExecutorsComplete() {

		for (ShardDiffTaskExecutor e : shardDiffTaskExecutors.values()) {
			if (!e.isComplete()) {
				return false;
			}
		}
		return true;
	}

	public void run() {

		loadChunks();
		initializeTasks();

		AtomicBoolean retryTaskPoolListenerDone = new AtomicBoolean(false);
		AtomicBoolean retryTaskPoolCollectorDone = new AtomicBoolean(false);
		AtomicBoolean retryTaskPoolDone = new AtomicBoolean(false);

		int totalChunks = getTotalChunks() + alreadyCompletedCount;
		int numShards = srcShardNames.size();

		summary.setTotalChunks(totalChunks);
		retryQueue = new DelayQueue<>();

		ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
		statusReporter.scheduleAtFixedRate(() -> logger.info(summary.getSummary(false)), 0, 5, TimeUnit.SECONDS);

		ThreadFactory retryTaskPoolThreadFactory = new ThreadFactoryBuilder().setNameFormat("RetryPool-%d").build();
		retryTaskPool = Executors.newFixedThreadPool(4, retryTaskPoolThreadFactory);
		List<Future<DiffResult>> retryTaskPoolFutures = new CopyOnWriteArrayList<>();

		/*
		 * Once the Retryer thread has seen all the END_TOKENs this value is set. It is
		 * used as a barrier to determine when to stop the retryPool
		 */
		AtomicInteger finalSizeRetryTaskPoolFutures = new AtomicInteger(-1);

		ScheduledExecutorService retryTaskPoolListener = Executors.newSingleThreadScheduledExecutor();
		retryTaskPoolListener.scheduleWithFixedDelay(new Runnable() {
			int endTokensSeen = 0;

			@Override
			public void run() {
				ShardRetryTask rt;
				while ((rt = (ShardRetryTask) retryQueue.poll()) != null) {
					try {
						if (rt == ShardRetryTask.END_TOKEN) {
							logger.trace("[RetryTaskPoolListener] saw an end token ({}/{})", endTokensSeen + 1,
									totalInitialTasks);
							if (++endTokensSeen == totalInitialTasks) {
								logger.debug("[RetryTaskPoolListener] has seen all end tokens ({})", endTokensSeen);
								finalSizeRetryTaskPoolFutures.set(retryTaskPoolFutures.size());
								retryTaskPoolListenerDone.set(true);
							}
						} else {
							logger.debug("[RetryTaskPoolListener] submitting retry {} for ({}-{})",
									rt.getRetryStatus().getAttempt() + 1, rt.getChunkDef().unitString());
							retryTaskPoolFutures.add(retryTaskPool.submit(rt));
						}
					} catch (Exception e) {
						logger.error("[RetryTaskPoolListener] Exception occured while running retry task", e);
						throw new RuntimeException(e);
					}

				}
			}
		}, 0, 1, TimeUnit.SECONDS);

		List<Collection> unshardedCollections = new ArrayList<>(
				sourceShardClient.getDatabaseCatalog().getUnshardedCollections());
		for (int i = 0; i < unshardedCollections.size(); i++) {
			Collection unshardedColl = unshardedCollections.get(i);

			// Round-robin which pool to assign to
			int shardIdx = i % numShards;
			String srcShard = srcShardNames.get(shardIdx);
			String destShard = destShardNames.get(shardIdx);
			ShardDiffTask task = new ShardDiffTask(config, null, unshardedColl.getNamespace(), srcShard, destShard,
					retryQueue, summary);
			logger.debug("[Main] Added an UnshardedDiffTask for {}", unshardedColl.getNamespace());
			// List<Future<DiffResult>> initialTaskPoolFutures =
			// initialTaskPoolFutureMap.get(srcShard);
			ThreadPoolExecutor initialTaskPool = initialTaskPoolMap.get(srcShard);
			// initialTaskPoolFutures.add(initialTaskPool.submit(task));
		}

		// Set<String> finishedShards = new HashSet<>();
		// Map<String, Set<Future<DiffResult>>> initialTaskPoolFuturesSeen = new
		// HashMap<>();
		// Set<Future<DiffResult>> initialTaskPoolFuturesSeen =
		// ConcurrentHashMap.newKeySet();
		Set<Future<DiffResult>> retryTaskPoolFuturesSeen = ConcurrentHashMap.newKeySet();

		// Check for futures coming off the retryPool

		ScheduledExecutorService retryTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
		Future<?> retryTaskPoolFuture = retryTaskPoolCollector.scheduleWithFixedDelay(new Runnable() {
			int runs = 0;

			@Override
			public void run() {
				int expectedRetryResults = finalSizeRetryTaskPoolFutures.get();
				logger.debug("[RetryTaskPoolCollector] loop: {} :: {} expected retryResults, {} seen", ++runs,
						expectedRetryResults, retryTaskPoolFuturesSeen.size());
				if (expectedRetryResults >= 0 && retryTaskPoolFuturesSeen.size() < expectedRetryResults) {
					for (Future<DiffResult> future : retryTaskPoolFutures) {
						try {
							if (!retryTaskPoolFuturesSeen.contains(future) && future.isDone()) {
								retryTaskPoolFuturesSeen.add(future);
								DiffResult result = future.get();
								if (result == null) {
									continue;
								}
								int failures = result.getFailedKeys().size();

								if (failures > 0 && result.isRetryable()) {
									// There's failures but will retry
									logger.trace(
											"[RetryTaskPoolCollector] ignoring retried result for ({}): "
													+ "{} matches, {} failures, {} bytes",
											result.getChunkDef().unitString(), result.getMatches(),
											result.getFailedKeys().size(), result.getBytesProcessed());
									continue;
								}

								logger.debug(
										"[RetryTaskPoolCollector] got final result for ({}): "
												+ "{} matches, {} failures, {} bytes",
										result.getChunkDef().unitString(), result.getMatches(),
										result.getFailedKeys().size(), result.getBytesProcessed());

								summary.updateRetryTask(result);
							}

						} catch (InterruptedException e) {
							logger.error("[RetryTaskPoolCollector] Diff task was interrupted", e);
							throw new RuntimeException(e);
						} catch (ExecutionException e) {
							logger.error("[RetryTaskPoolCollector] Diff task threw an exception", e);
							throw new RuntimeException(e);
						}
					}
				} else {
					retryTaskPoolCollectorDone.set(true);
				}
//                }
			}

		}, 0, 1, TimeUnit.SECONDS);

		boolean retryTaskPoolResult = false;

		while (!(retryTaskPoolDone.get() && shardDiffTaskExecutorsComplete())) {
			try {
				Thread.sleep(1000);
				logger.debug(
						"[Main] check completion status: shardDiffTaskExecutorsComplete: {}, retryTaskPoolDone: {}",
						shardDiffTaskExecutorsComplete(), retryTaskPoolDone.get());

				if (!retryTaskPoolDone.get()) {
					if (retryTaskPoolListenerDone.get() && !retryTaskPoolListener.isShutdown()) {
						logger.info("[Main] shutting down retry task pool listener");
						retryTaskPoolListener.shutdown();
					} else {
						logger.trace("[Main] retry pool listener still running");
					}
					if (retryTaskPoolCollectorDone.get() && !retryTaskPoolCollector.isShutdown()) {
						logger.info("[Main] shutting down retry task pool collector");
						retryTaskPoolResult = retryTaskPoolFuture.cancel(false);
						retryTaskPoolCollector.shutdown();
					} else {
						logger.trace("[Main] retry pool collector still running");
					}
					if (retryTaskPoolResult) {
						retryTaskPoolDone.set(true);
						logger.info("[Main] shutting down retry task pool");
						retryTaskPool.shutdown();
						if (!retryTaskPoolListener.isShutdown()) {
							retryTaskPoolListener.shutdown();
						}
					}
				} else {
					logger.trace("[Main] retry pool still running");
				}
//                if (!initialTaskPoolDone.get()) {
//                    if (initialTaskPoolCollectorDone.get() && !initialTaskPoolCollector.isShutdown()) {
//                        logger.info("[Main] shutting down initial task pool collector");
//                        initialTaskPoolResult = initialTaskPoolFuture.cancel(false);
//                        initialTaskPoolCollector.shutdown();
//                    } else {
//                        logger.trace("[Main] Initial task pool collector still running");
//                    }
//                    if (initialTaskPoolResult) {
//                        initialTaskPoolDone.set(true);
//                        logger.info("[Main] shutting down {} initial task pools", initialTaskPoolMap.size());
//                        for (ThreadPoolExecutor e : initialTaskPoolMap.values()) {
//                            e.shutdown();
//                        }
//                    }
//                } else {
//                    logger.trace("[Main] Initial task pool still running");
//                }
			} catch (Exception e) {
				logger.error("[Main] Error collector worker and/or retry pool", e);
				throw new RuntimeException(e);
			}
		}

		logger.info("[Main] shutting down statusReporter thread");
		statusReporter.shutdown();

		logger.info(summary.getSummary(true));

	}

}
