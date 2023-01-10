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

import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.diff3.DiffConfiguration;
//import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
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

    private final DiffConfiguration config;

    protected Map<String, ThreadPoolExecutor> initialTaskPoolMap = new HashMap<>();
    private ExecutorService retryTaskPool;
    Map<String, List<Future<DiffResult>>> initialTaskPoolFutureMap = new HashMap<>();

    private final Map<String, Map<String, RawBsonDocument>> sourceChunksCacheMap;
    private final long estimatedTotalDocs;
    private final long totalSize;
    private final int numUnshardedCollections;
    private Queue<RetryTask> retryQueue;
    private int totalInitialTasks;
    
    private ChunkManager chunkManager;


    public ShardDiffUtil(DiffConfiguration config) {

        this.config = config;

        chunkManager = new ChunkManager(config);
        chunkManager.initalize();
        
        
        this.sourceShardClient = config.getSourceShardClient();
		this.destShardClient = config.getDestShardClient();

        Set<String> includeNs = config.getIncludeNamespaces().stream()
                .map(Namespace::getNamespace).collect(Collectors.toSet());
        sourceShardClient.populateCollectionsMap(includeNs);
        DatabaseCatalog catalog = sourceShardClient.getDatabaseCatalog(config.getIncludeNamespaces());

        long[] sizeAndCount = catalog.getTotalSizeAndCount();
        totalSize = sizeAndCount[0];
        estimatedTotalDocs = sizeAndCount[1];

        Set<String> shardedColls = catalog.getShardedCollections().stream()
                .map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());
        Set<String> unshardedColls = catalog.getUnshardedCollections().stream()
                .map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());
        numUnshardedCollections = unshardedColls.size();

        logger.info("[Main] shardedColls:[" + String.join(", ", shardedColls) + "]");

        logger.info("[Main] unshardedColls:[" + String.join(", ", unshardedColls) + "]");
        sourceChunksCacheMap = sourceShardClient.loadChunksCacheMap(chunkManager.getChunkQuery());

        sourceShardClient.populateShardMongoClients();
        destShardClient.populateShardMongoClients();

        srcShardNames = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
        destShardNames = new ArrayList<>(destShardClient.getShardsMap().keySet());

        for (String shard : srcShardNames) {
            int numThreads = config.getThreads() / srcShardNames.size();
            Map<String, RawBsonDocument> chunkMap = sourceChunksCacheMap.get(shard);
//            int qSize = chunkMap.size() + unshardedColls.size();
            int qSize = chunkMap.size();
            totalInitialTasks += qSize;
            logger.debug("[Main] Setting workQueue size to {}", qSize);
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(qSize);
            initialTaskPoolFutureMap.put(shard, new ArrayList<>(chunkMap.size()));
            ThreadFactory initialTaskPoolThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("WorkerPool-" + shard + "-%d").build();
            ThreadPoolExecutor initialTaskPool = new ThreadPoolExecutor(numThreads, numThreads, 30, TimeUnit.SECONDS,
                    workQueue, new BlockWhenQueueFull());
            initialTaskPool.setThreadFactory(initialTaskPoolThreadFactory);
            initialTaskPoolMap.put(shard, initialTaskPool);
        }
        totalInitialTasks += unshardedColls.size();

        logger.info("[Main] found {} initial tasks", totalInitialTasks);
    }

    private int getTotalChunks() {
        AtomicInteger sum = new AtomicInteger();
        sourceChunksCacheMap.forEach((k, v) -> sum.addAndGet(v.size()));
        return sum.get() + numUnshardedCollections;
    }

    public void run() {
        AtomicBoolean retryTaskPoolListenerDone = new AtomicBoolean(false);
        AtomicBoolean retryTaskPoolCollectorDone = new AtomicBoolean(false);
        AtomicBoolean initialTaskPoolCollectorDone = new AtomicBoolean(false);
        AtomicBoolean retryTaskPoolDone = new AtomicBoolean(false);
        AtomicBoolean initialTaskPoolDone = new AtomicBoolean(false);

        int totalChunks = getTotalChunks();
        int numShards = srcShardNames.size();

        // Initialize diff summary (optionally with db storage)
        DiffSummaryClient diffSummaryClient = null;
        if (config.isUseStatusDb()) {
            diffSummaryClient = new DiffSummaryClient(config.getStatusDbUri(), config.getStatusDbName(),
                    config.getStatusDbCollName());
        }
        DiffSummary summary = new DiffSummary(estimatedTotalDocs, totalSize, diffSummaryClient);
        summary.setTotalChunks(totalChunks);
//        retryQueue = new LinkedBlockingQueue<>();
        retryQueue = new DelayQueue<>();

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(() -> logger.info(summary.getSummary(false)), 0, 5, TimeUnit.SECONDS);

        ThreadFactory retryTaskPoolThreadFactory = new ThreadFactoryBuilder().setNameFormat("RetryPool-%d").build();
        retryTaskPool = Executors.newFixedThreadPool(4, retryTaskPoolThreadFactory);
        List<Future<DiffResult>> retryTaskPoolFutures = new CopyOnWriteArrayList<>();

        /* Once the Retryer thread has seen all the END_TOKENs this value is set.
         *  It is used as a barrier to determine when to stop the retryPool */
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
                            logger.trace("[RetryTaskPoolListener] saw an end token ({}/{})",
                                    endTokensSeen + 1, totalInitialTasks);
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

        for (int i = 0; i < srcShardNames.size(); i++) {
            String srcShard = srcShardNames.get(i);
            String destShard = chunkManager.getShardMapping(srcShard);
            //String destShard = destShardNames.get(i);
            Map<String, RawBsonDocument> chunkCache = sourceChunksCacheMap.get(srcShard);
            for (RawBsonDocument chunk : chunkCache.values()) {
                String nsStr = chunk.get("ns").asString().getValue();
                ShardDiffTask task = new ShardDiffTask(sourceShardClient, destShardClient,
                        config, chunk, new Namespace(nsStr), srcShard, destShard, retryQueue, summary);
                List<Future<DiffResult>> initialTaskPoolFutures = initialTaskPoolFutureMap.get(srcShard);
                ThreadPoolExecutor initialTaskPool = initialTaskPoolMap.get(srcShard);
                initialTaskPoolFutures.add(initialTaskPool.submit(task));
            }
        }

        List<Collection> unshardedCollections = new ArrayList<>(
                sourceShardClient.getDatabaseCatalog().getUnshardedCollections());
        for (int i = 0; i < unshardedCollections.size(); i++) {
            Collection unshardedColl = unshardedCollections.get(i);

            // Round-robin which pool to assign to
            int shardIdx = i % numShards;
            String srcShard = srcShardNames.get(shardIdx);
            String destShard = destShardNames.get(shardIdx);
            ShardDiffTask task = new ShardDiffTask(sourceShardClient, destShardClient, config, null,
                    unshardedColl.getNamespace(), srcShard, destShard, retryQueue, summary);
            logger.debug("[Main] Added an UnshardedDiffTask for {}", unshardedColl.getNamespace());
            List<Future<DiffResult>> initialTaskPoolFutures = initialTaskPoolFutureMap.get(srcShard);
            ThreadPoolExecutor initialTaskPool = initialTaskPoolMap.get(srcShard);
            initialTaskPoolFutures.add(initialTaskPool.submit(task));
        }

        Set<String> finishedShards = new HashSet<>();
        Map<String, Set<Future<DiffResult>>> initialTaskPoolFuturesSeen = new HashMap<>();
        Set<Future<DiffResult>> retryTaskPoolFuturesSeen = ConcurrentHashMap.newKeySet();

        // Check for futures coming off the retryPool

        ScheduledExecutorService retryTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
        Future<?> retryTaskPoolFuture = retryTaskPoolCollector.scheduleWithFixedDelay(new Runnable() {
            int runs = 0;

            @Override
            public void run() {
                int expectedRetryResults = finalSizeRetryTaskPoolFutures.get();
                logger.trace("[RetryTaskPoolCollector] loop: {} :: {} expected retryResults, {} seen",
                        ++runs, expectedRetryResults, retryTaskPoolFuturesSeen.size());
//                if (expectedRetryResults >= 0) {
                    if (expectedRetryResults < 0 || (expectedRetryResults >= 0 && retryTaskPoolFuturesSeen.size() < expectedRetryResults)) {
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
                                        logger.trace("[RetryTaskPoolCollector] ignoring retried result for ({}): " +
                                                        "{} matches, {} failures, {} bytes",
                                                result.getChunkDef().unitString(), result.getMatches(),
                                                result.getFailedKeys().size(), result.getBytesProcessed());
                                        continue;
                                    }

                                    logger.debug("[RetryTaskPoolCollector] got final result for ({}): " +
                                                    "{} matches, {} failures, {} bytes",
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

        ScheduledExecutorService initialTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
        Future<?> initialTaskPoolFuture = initialTaskPoolCollector.scheduleAtFixedRate(new Runnable() {
            int runs = 0;

            @Override
            public void run() {
                // Poll for completed futures every second
                if (finishedShards.size() < numShards) {
                    logger.trace("[InitialTaskPoolCollector] loop: {}", ++runs);
                    for (String shard : srcShardNames) {
                        if (!initialTaskPoolFuturesSeen.containsKey(shard)) {
                            initialTaskPoolFuturesSeen.put(shard, new HashSet<>());
                        }
                        Set<Future<DiffResult>> futuresSeen = initialTaskPoolFuturesSeen.get(shard);
                        List<Future<DiffResult>> diffResults = initialTaskPoolFutureMap.get(shard);

                        if (futuresSeen.size() >= diffResults.size()) {
                            finishedShards.add(shard);
                        } else {
                            for (Future<DiffResult> future : diffResults) {
                                try {
                                    if (!futuresSeen.contains(future) && future.isDone()) {
                                        futuresSeen.add(future);

                                        DiffResult result = future.get();
                                        int failures = result.getFailedKeys().size();
                                        if (failures > 0) {
                                            logger.debug("[InitialTaskPoolCollector] will retry {} failed ids for ({})",
                                                    result.getFailedKeys().size(), result.getChunkDef().unitString());
                                        } else {
                                            logger.trace("[InitialTaskPoolCollector] got result for ({}): " +
                                                            "{} matches, {} failures, {} bytes",
                                                    result.getChunkDef().unitString(), result.getMatches(),
                                                    result.getFailedKeys().size(), result.getBytesProcessed());
                                        }
                                        summary.updateInitTask(result);
                                    }
                                } catch (InterruptedException e) {
                                    logger.error("[InitialTaskPoolCollector] Diff task was interrupted", e);
                                } catch (ExecutionException e) {
                                    logger.error("[InitialTaskPoolCollector] Diff task threw an exception", e);
                                }
                            }
                        }
                    }
                } else {
                    initialTaskPoolCollectorDone.set(true);
                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        boolean initialTaskPoolResult = false;
        boolean retryTaskPoolResult = false;

        while (!(retryTaskPoolDone.get() && initialTaskPoolDone.get())) {
            try {
                Thread.sleep(1000);
                logger.trace("[Main] check completion status");

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
                if (!initialTaskPoolDone.get()) {
                    if (initialTaskPoolCollectorDone.get() && !initialTaskPoolCollector.isShutdown()) {
                        logger.info("[Main] shutting down initial task pool collector");
                        initialTaskPoolResult = initialTaskPoolFuture.cancel(false);
                        initialTaskPoolCollector.shutdown();
                    } else {
                        logger.trace("[Main] Initial task pool collector still running");
                    }
                    if (initialTaskPoolResult) {
                        initialTaskPoolDone.set(true);
                        logger.info("[Main] shutting down {} initial task pools", initialTaskPoolMap.size());
                        for (ThreadPoolExecutor e : initialTaskPoolMap.values()) {
                            e.shutdown();
                        }
                    }
                } else {
                    logger.trace("[Main] Initial task pool still running");
                }
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
