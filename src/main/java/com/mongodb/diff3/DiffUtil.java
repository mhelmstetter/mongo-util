package com.mongodb.diff3;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DiffUtil {

    private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);
    private List<String> destShardNames;
    private List<String> srcShardNames;

    private ShardClient sourceShardClient;
    private ShardClient destShardClient;

    private DiffConfiguration config;

    protected Map<String, ThreadPoolExecutor> executorMap = new HashMap<>();
    private ExecutorService retryPool;
    Map<String, List<Future<DiffResult>>> diffResultMap = new HashMap<>();

    private Map<String, Map<String, RawBsonDocument>> sourceChunksCacheMap = new HashMap<>();
    private long estimatedTotalDocs;
    private long totalSize;
    private int numUnshardedCollections;
    private Queue<RetryTask> retryQueue;
    private int totalInitialTasks;


    public DiffUtil(DiffConfiguration config) {

        this.config = config;


        sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
        destShardClient = new ShardClient("dest", config.getDestClusterUri());

        sourceShardClient.init();
        destShardClient.init();

        Set<String> includeNs = config.getIncludeNamespaces().stream()
                .map(n -> n.getNamespace()).collect(Collectors.toSet());
        sourceShardClient.populateCollectionsMap(includeNs);
        DatabaseCatalog catalog = sourceShardClient.getDatabaseCatalog(config.getIncludeNamespaces());

        long[] sizeAndCount = catalog.getTotalSizeAndCount();
        totalSize = sizeAndCount[0];
        estimatedTotalDocs = sizeAndCount[1];

        Set<String> shardedColls = catalog.getShardedCollections().stream()
                .map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());
        Set<String> unshardedColls = catalog.getUnshardedCollections().stream()
                .map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());

        logger.info("[Main] shardedColls:[" + String.join(", ", shardedColls) + "]");

        logger.info("[Main] unshardedColls:[" + String.join(", ", unshardedColls) + "]");
        sourceChunksCacheMap = sourceShardClient.loadChunksCacheMap(config.getChunkQuery());

        sourceShardClient.populateShardMongoClients();
        destShardClient.populateShardMongoClients();

        srcShardNames = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
        destShardNames = new ArrayList<>(destShardClient.getShardsMap().keySet());

        for (String shard : srcShardNames) {
            int numThreads = config.getThreads() / srcShardNames.size();
            Map<String, RawBsonDocument> chunkMap = sourceChunksCacheMap.get(shard);
            int qSize = chunkMap.size() + unshardedColls.size();
            totalInitialTasks += qSize;
            logger.debug("[Main] Setting workQueue size to {}", qSize);
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(qSize);
            diffResultMap.put(shard, new ArrayList<>(chunkMap.size()));
            ThreadFactory workerPoolThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("WorkerPool-" + shard + "-%d").build();
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(numThreads, numThreads, 30, TimeUnit.SECONDS,
                    workQueue, new BlockWhenQueueFull());
            tpe.setThreadFactory(workerPoolThreadFactory);
            executorMap.put(shard, tpe);
        }

        logger.info("[Main] found {} initial tasks", totalInitialTasks);
    }

    private int getTotalChunks() {
        AtomicInteger sum = new AtomicInteger();
        sourceChunksCacheMap.forEach((k, v) -> {
            sum.addAndGet(v.size());
        });
        return sum.get() + numUnshardedCollections;
    }

    public void run() {
        int totalChunks = getTotalChunks();
        int numShards = srcShardNames.size();
        DiffSummary summary = new DiffSummary(estimatedTotalDocs, totalSize);
        summary.setTotalChunks(totalChunks);
        retryQueue = new LinkedBlockingQueue<>();

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info(summary.getSummary(false));
            }
        }, 0, 5, TimeUnit.SECONDS);

        ThreadFactory retryThreadFactory = new ThreadFactoryBuilder().setNameFormat("RetryPool-%d").build();
        retryPool = Executors.newFixedThreadPool(4, retryThreadFactory);
        List<Future<DiffResult>> retryResults = new CopyOnWriteArrayList<>();

        /* Once the Retryer thread has seen all the END_TOKENs this value is set.
        *  It is used as a barrier to determine when to stop the retryPool */
        AtomicInteger finalSizeRetryResults = new AtomicInteger(-1);

        ScheduledExecutorService failedTaskListener = Executors.newSingleThreadScheduledExecutor();
        failedTaskListener.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;

            @Override
            public void run() {
                RetryTask rt;
                while ((rt = retryQueue.poll()) != null) {
                    if (rt != null) {
                        try {
                            if (rt == RetryTask.END_TOKEN) {
                                logger.trace("[FailedTaskListener] saw an end token ({}/{})",
                                        endTokensSeen + 1, totalInitialTasks);
                                if (++endTokensSeen == totalInitialTasks) {
                                    logger.debug("[FailedTaskListener] has seen all end tokens ({})", endTokensSeen);
                                    finalSizeRetryResults.set(retryResults.size());
                                }
                            } else {
                                DiffTask originalTask = rt.getOriginalTask();
                                logger.debug("[FailedTaskListener] submitting retry {} for ({}-{})",
                                        rt.getRetryStatus().getAttempt() + 1, originalTask.getNamespace(),
                                        originalTask.chunkString);
                                retryResults.add(retryPool.submit(rt));
                            }
                        } catch (Exception e) {
                            logger.error("[FailedTaskListener] Exception occured while running retry task", e);
                            throw new RuntimeException(e);
                        }
                    }

                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        for (int i = 0; i < srcShardNames.size(); i++) {
            String srcShard = srcShardNames.get(i);
            String destShard = destShardNames.get(i);
            Map<String, RawBsonDocument> chunkCache = sourceChunksCacheMap.get(srcShard);
            for (RawBsonDocument chunk : chunkCache.values()) {
                String nsStr = chunk.get("ns").asString().getValue();
                DiffTask task = new DiffTask(sourceShardClient, destShardClient,
                        config, chunk, nsStr, srcShard, destShard, retryQueue);
                List<Future<DiffResult>> diffResults = diffResultMap.get(srcShard);
                ThreadPoolExecutor executor = executorMap.get(srcShard);
                diffResults.add(executor.submit(task));
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
            DiffTask task = new DiffTask(sourceShardClient, destShardClient, config, null,
                    unshardedColl.getNamespace().getNamespace(), srcShard, destShard, retryQueue);
            logger.debug("[Main] Added an UnshardedDiffTask for {}", unshardedColl.getNamespace());
            List<Future<DiffResult>> diffResults = diffResultMap.get(srcShard);
            ThreadPoolExecutor executor = executorMap.get(srcShard);
            diffResults.add(executor.submit(task));
        }

        Set<String> finishedShards = new HashSet<>();
        Map<String, Set<Future<DiffResult>>> futSeenMap = new HashMap<>();
        Set<Future<DiffResult>> retriesSeen = ConcurrentHashMap.newKeySet();

        // Check for futures coming off the retryPool

        ScheduledExecutorService retryResultCollector = Executors.newSingleThreadScheduledExecutor();
        Future<?> retryPoolResultFuture = retryResultCollector.submit(new Runnable() {
            @Override
            public void run() {
                int runs = 0;
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    int expectedRetryResults = finalSizeRetryResults.get();
                    logger.trace("[RetryResultCollector] loop: {} :: {} expected retryResults, {} seen",
                            ++runs, expectedRetryResults, retriesSeen.size());
                    if (expectedRetryResults >= 0) {
                        if (retriesSeen.size() < expectedRetryResults) {
                            for (Future<DiffResult> future : retryResults) {
                                try {
                                    if (!retriesSeen.contains(future) && future.isDone()) {
                                        retriesSeen.add(future);
                                        DiffResult result = future.get();
                                        int failures = result.getFailureCount();

                                        if (failures > 0 && result.isRetryable()) {
                                            // There's failures but will retry
                                            logger.trace("[ResultRetryCollector] ignoring retried result for ({}): " +
                                                            "{} matches, {} failures, {} bytes",
                                                    result.getNs(), result.getChunkString(), result.getMatches(),
                                                    result.getFailureCount(), result.getBytesProcessed());
                                            continue;
                                        }

                                        logger.debug("[ResultRetryCollector] got final result for ({}-{}): " +
                                                        "{} matches, {} failures, {} bytes",
                                                result.getNs(), result.getChunkString(), result.getMatches(),
                                                result.getFailureCount(), result.getBytesProcessed());

                                        summary.updateRetryTask(result);
                                    }

                                } catch (InterruptedException e) {
                                    logger.error("[ResultRetryCollector] Diff task was interrupted", e);
                                    throw new RuntimeException(e);
                                } catch (ExecutionException e) {
                                    logger.error("[ResultRetryCollector] Diff task threw an exception", e);
                                    throw new RuntimeException(e);
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
            }
        });

        ScheduledExecutorService workerPoolResultCollector = Executors.newSingleThreadScheduledExecutor();
        Future<?> workerPoolResultFuture = workerPoolResultCollector.submit(new Runnable() {
            @Override
            public void run() {
                int runs = 0;
                // Poll for completed futures every second
                while (finishedShards.size() < numShards) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    logger.trace("[WorkerResultCollector] loop: {}", ++runs);
                    for (String shard : srcShardNames) {
                        if (!futSeenMap.containsKey(shard)) {
                            futSeenMap.put(shard, new HashSet<>());
                        }
                        Set<Future<DiffResult>> futuresSeen = futSeenMap.get(shard);
                        List<Future<DiffResult>> diffResults = diffResultMap.get(shard);

                        if (futuresSeen.size() >= diffResults.size()) {
                            finishedShards.add(shard);
                        } else {
                            for (Future<DiffResult> future : diffResults) {
                                try {
                                    if (!futuresSeen.contains(future) && future.isDone()) {
                                        futuresSeen.add(future);

                                        DiffResult result = future.get();
                                        int failures = result.getFailureCount();
                                        if (failures > 0) {
                                            logger.debug("[WorkerResultCollector] will retry {} failed ids for {}--{} ({})",
                                                    result.getFailureCount(), result.getNs(), shard, result.getChunkString());
                                        } else {
                                            logger.debug("[WorkerResultCollector] got result for {}--{} ({}): " +
                                                            "{} matches, {} failures, {} bytes",
                                                    result.getNs(), shard, result.getChunkString(), result.getMatches(),
                                                    result.getFailureCount(), result.getBytesProcessed());
                                        }
                                        summary.updateInitTask(result);
                                    }

//                logger.debug("result: {}", result);
                                } catch (InterruptedException e) {
                                    logger.error("[WorkerResultCollector] Diff task was interrupted", e);
                                } catch (ExecutionException e) {
                                    logger.error("[WorkerResultCollector] Diff task threw an exception", e);
                                }
                            }
                        }
                    }
                }
            }
        });

        boolean retryPoolDone = false;
        boolean workerPoolDone = false;

        while (!(retryPoolDone && workerPoolDone)) {
            try {
                Thread.sleep(100);

                if (!retryPoolDone && retryPoolResultFuture.isDone()) {
                    retryPoolResultFuture.get();
                    logger.info("[Main] Retry pool is done");
                    retryPoolDone = true;
                }
                if (!workerPoolDone && workerPoolResultFuture.isDone()) {
                    workerPoolResultFuture.get();
                    logger.info("[Main] Worker pool is done");
                    workerPoolDone = true;
                }
            } catch (Exception e) {
                logger.error("[Main] Error collector worker and/or retry pool", e);
                throw new RuntimeException(e);
            }
        }

        logger.info("[Main] shutting down workerPoolResultCollector thread");
        workerPoolResultCollector.shutdown();

        logger.info("[Main] shutting down retryResultCollector thread");
        retryResultCollector.shutdown();

        logger.info("[Main] shutting down failedTaskListener thread");
        failedTaskListener.shutdown();

        logger.info("[Main] shutting down statusReporter thread");
        statusReporter.shutdown();

        logger.info("[Main] shutting down retry pool");
        retryPool.shutdown();

        logger.info("[Main] shutting down {} worker pools", executorMap.size());
        for (ThreadPoolExecutor executor : executorMap.values()) {
            executor.shutdown();
        }
        logger.info(summary.getSummary(true));
    }

}
