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
                .map(c -> c.getNamespace()).collect(Collectors.toSet());
        Set<String> unshardedColls = catalog.getUnshardedCollections().stream()
                .map(c -> c.getNamespace()).collect(Collectors.toSet());

        logger.info("ShardedColls:[" + String.join(", ", shardedColls) + "]");

        logger.info("UnshardedColls:[" + String.join(", ", unshardedColls) + "]");
        sourceChunksCacheMap = sourceShardClient.loadChunksCacheMap(config.getChunkQuery());
//        shardNames = new ArrayList<>(sourceChunksCacheMap.keySet());
//        int numShards = shardNames.size();

        sourceShardClient.populateShardMongoClients();
        destShardClient.populateShardMongoClients();

        srcShardNames = new ArrayList<>(sourceShardClient.getShardsMap().keySet());
        destShardNames = new ArrayList<>(destShardClient.getShardsMap().keySet());

        for (String shard : srcShardNames) {
            int numThreads = config.getThreads() / srcShardNames.size();
            Map<String, RawBsonDocument> chunkMap = sourceChunksCacheMap.get(shard);
            int qSize = chunkMap.size() + unshardedColls.size();
            totalInitialTasks += qSize;
            logger.debug("Setting workQueue size to {}", qSize);
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(qSize);
            diffResultMap.put(shard, new ArrayList<>(chunkMap.size()));
            ThreadFactory workerPoolThreadFactory =
                    new ThreadFactoryBuilder().setNameFormat("WorkerPool-" + shard + "-%d").build();
            ThreadPoolExecutor tpe = new ThreadPoolExecutor(numThreads, numThreads, 30, TimeUnit.SECONDS,
                    workQueue, new BlockWhenQueueFull());
            tpe.setThreadFactory(workerPoolThreadFactory);
            executorMap.put(shard, tpe);
        }

        logger.info("Found {} initial tasks", totalInitialTasks);
    }

    private int getTotalChunks() {
        AtomicInteger sum = new AtomicInteger();
        sourceChunksCacheMap.forEach((k, v) -> {
            sum.addAndGet(v.size());
        });
        return sum.get();
    }

    public void run() {
        int totalChunks = getTotalChunks();
        int numShards = srcShardNames.size();
        DiffSummary summary = new DiffSummary(totalChunks, estimatedTotalDocs, totalSize);
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
        AtomicInteger finalSizeRetryResults = new AtomicInteger(-1);

        ScheduledExecutorService retryer = Executors.newSingleThreadScheduledExecutor();
        retryer.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;

            @Override
            public void run() {
                RetryTask rt;
                while ((rt = retryQueue.poll()) != null) {
                    if (rt != null) {
                        try {
                            if (rt == RetryTask.END_TOKEN) {
                                logger.debug("[Retryer] saw an end token ({}/{})",
                                        endTokensSeen + 1, totalInitialTasks);
                                if (++endTokensSeen == totalInitialTasks) {
                                    logger.debug("[Retryer] has seen all end tokens ({})", endTokensSeen);
                                    finalSizeRetryResults.set(retryResults.size());
                                }
                            } else {
                                AbstractDiffTask originalTask = rt.getOriginalTask();
                                logger.debug("[Retryer] submitting retry {} for ({}-{})",
                                        rt.getRetryStatus().getAttempt() + 1, originalTask.namespace.getNamespace(),
                                        originalTask.chunkString);
                                retryResults.add(retryPool.submit(rt));
                            }
                        } catch (Exception e) {
                            logger.error("Exception occured while running retry task", e);
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
                ShardedDiffTask task = new ShardedDiffTask(sourceShardClient, destShardClient,
                        config, chunk, srcShard, destShard, retryQueue);
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
            UnshardedDiffTask task = new UnshardedDiffTask(sourceShardClient, destShardClient,
                    unshardedColl.getNamespace(), srcShard, destShard, retryQueue);
            logger.debug("Added an UnshardedDiffTask for {}", unshardedColl.getNamespace());
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
                    logger.debug("[RetryResultCollector] loop: {} :: {} expected retryResults, {} seen",
                            ++runs, expectedRetryResults, retriesSeen.size());
                    if (expectedRetryResults >= 0) {
                        if (retriesSeen.size() < expectedRetryResults) {
                            for (Future<DiffResult> future : retryResults) {
                                try {
                                    if (!retriesSeen.contains(future) && future.isDone()) {
                                        retriesSeen.add(future);
                                        DiffResult result = future.get();
                                        int failures = result.getFailureCount();

                                        logger.debug("[ResultRetryCollector] got result for ({}-{}): " +
                                                        "{} matches, {} failures, {} bytes",
                                                result.ns, result.chunkString, result.matches,
                                                result.getFailureCount(), result.bytesProcessed);

                                        if (failures > 0) {
                                            if (result instanceof ShardedDiffResult) {
                                                summary.incrementFailedChunks(1);
                                            }
                                            summary.incrementSuccessfulDocs(result.matches - failures);
                                        } else {
                                            if (result instanceof ShardedDiffResult) {
                                                summary.incrementSuccessfulChunks(1);
                                            }
                                            summary.incrementSuccessfulDocs(result.matches);
                                        }

                                        summary.incrementRetryChunks(-1);
                                        summary.incrementProcessedDocs(result.matches + failures);
                                        summary.incrementFailedDocs(failures);
                                        if (result instanceof ShardedDiffResult) {
                                            summary.incrementProcessedChunks(1);
                                        }
                                        summary.incrementSourceOnly(result.onlyOnSource);
                                        summary.incrementDestOnly(result.onlyOnDest);
                                        summary.incrementProcessedSize(result.bytesProcessed);
                                    }

                                } catch (InterruptedException e) {
                                    logger.error("Diff task was interrupted", e);
                                    throw new RuntimeException(e);
                                } catch (ExecutionException e) {
                                    logger.error("Diff task threw an exception", e);
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

                    logger.debug("WorkerResultCollector loop: {}", ++runs);
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
                                                    result.getFailureCount(), result.ns, shard, result.chunkString);
                                            summary.incrementRetryChunks(1);
                                        } else {
                                            logger.debug("[WorkerResultCollector] got result for {}--{} ({}): " +
                                                            "{} matches, {} failures, {} bytes",
                                                    result.ns, shard, result.chunkString, result.matches,
                                                    result.getFailureCount(), result.bytesProcessed);
                                        }
                                        summary.incrementSuccessfulDocs(result.matches);

                                        summary.incrementProcessedDocs(result.matches);
                                        if (result instanceof ShardedDiffResult) {
                                            summary.incrementProcessedChunks(1);
                                        }
                                        summary.incrementProcessedSize(result.bytesProcessed);
                                    }

//                logger.debug("result: {}", result);
                                } catch (InterruptedException e) {
                                    logger.error("Diff task was interrupted", e);
                                } catch (ExecutionException e) {
                                    logger.error("Diff task threw an exception", e);
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
                    logger.info("Retry pool is done");
                    retryPoolDone = true;
                }
                if (!workerPoolDone && workerPoolResultFuture.isDone()) {
                    workerPoolResultFuture.get();
                    logger.info("Worker pool is done");
                    workerPoolDone = true;
                }
            } catch (Exception e) {
                logger.error("Error collector worker and/or retry pool", e);
                throw new RuntimeException(e);
            }
        }

        logger.info("Shutting down workerPoolResultCollector thread");
        workerPoolResultCollector.shutdown();

        logger.info("Shutting down retryResultCollector thread");
        retryResultCollector.shutdown();

        logger.info("Shutting down retryer thread");
        retryer.shutdown();

        logger.info("Shutting down statusReporter thread");
        statusReporter.shutdown();

        logger.info("Shutting down retry pool");
        retryPool.shutdown();

        logger.info("Shutting down {} worker pools", executorMap.size());
        for (ThreadPoolExecutor executor : executorMap.values()) {
            executor.shutdown();
        }
        logger.info(summary.getSummary(true));
    }

}
