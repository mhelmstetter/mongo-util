package com.mongodb.diff3.partition;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.diff3.DiffConfiguration;
//import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffResult;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.DiffSummaryClient;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.DatabaseCatalogProvider;
import com.mongodb.model.StandardDatabaseCatalogProvider;
import com.mongodb.util.BlockWhenQueueFull;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class PartitionDiffUtil {
    private static final Logger logger = LoggerFactory.getLogger(PartitionDiffUtil.class);
    private final MongoClient sourceClient;
    private final MongoClient destClient;
    private final DatabaseCatalogProvider databaseCatalogProvider;
    private final PartitionManager partitionManager;
    private final DiffConfiguration config;
    private final ThreadPoolExecutor initialTaskPool;

    private final ThreadPoolExecutor partitionerTaskPool;
    private ExecutorService retryTaskPool;
    private final List<Future<DiffResult>> initialTaskPoolResults = new ArrayList<>();
    private final List<Future<Pair<String, Integer>>> partitionerTaskPoolResults = new ArrayList<>();
    private final long estimatedTotalDocs;
    private final long totalSize;
    private Queue<PartitionDiffTask> partitionerTaskPoolQueue;
    private Queue<RetryTask> retryQueue;

    public PartitionDiffUtil(DiffConfiguration config) {
        this.config = config;

        sourceClient = initClient(config.getSourceClusterUri());
        destClient = initClient(config.getDestClusterUri());

        databaseCatalogProvider = new StandardDatabaseCatalogProvider(sourceClient);
        partitionManager = new PartitionManager(config.getSampleRate(), config.getSampleMinDocs(),
                config.getMaxDocsToSamplePerPartition(), config.getDefaultPartitionSize());

        DatabaseCatalog catalog = databaseCatalogProvider.get(config.getIncludeNamespaces());
        long[] sizeAndCount = catalog.getTotalSizeAndCount();
        totalSize = sizeAndCount[0];
        estimatedTotalDocs = sizeAndCount[1];
        Set<String> colls = catalog.getUnshardedCollections().stream()
                .map(c -> c.getNamespace().getNamespace()).collect(Collectors.toSet());
        logger.info("[Main] collections: [" + String.join(", ", colls) + "]");

        int queueSize = colls.size() * 8;
        logger.debug("[Main] Setting queue size to {}", queueSize);
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueSize);
        BlockingQueue<Runnable> partitionerWorkQueue = new ArrayBlockingQueue<>(colls.size());
        int numThreads = config.getThreads();
        initialTaskPool = new ThreadPoolExecutor(numThreads, numThreads, 30,
                TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
        partitionerTaskPool = new ThreadPoolExecutor(Math.max(colls.size(), 16), Math.max(colls.size(), 16), 30,
                TimeUnit.SECONDS, partitionerWorkQueue, new BlockWhenQueueFull());
    }

    public void run() {
        AtomicBoolean partitionerTaskPoolDone = new AtomicBoolean(false);
        AtomicBoolean initialTaskPoolDone = new AtomicBoolean(false);
        AtomicBoolean retryTaskPoolDone = new AtomicBoolean(false);
        AtomicBoolean partitionerTaskPoolListenerDone = new AtomicBoolean(false);
        AtomicBoolean partitionerTaskPoolCollectorDone = new AtomicBoolean(false);
        AtomicBoolean retryTaskPoolCollectorDone = new AtomicBoolean(false);
        AtomicBoolean retryTaskPoolListenerDone = new AtomicBoolean(false);
        AtomicBoolean initialTaskPoolCollectorDone = new AtomicBoolean(false);

        Set<Collection> colls = databaseCatalogProvider.get().getUnshardedCollections();

        // Initialize diff summary (optionally with db storage)
        DiffSummaryClient diffSummaryClient = null;
        if (config.isUseStatusDb()) {
            diffSummaryClient = new DiffSummaryClient(config.getStatusDbUri(), config.getStatusDbName(),
                    config.getStatusDbCollName());
        }
        DiffSummary summary = new DiffSummary(estimatedTotalDocs, totalSize, diffSummaryClient);

        retryQueue = new DelayQueue<>();
        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(() ->
                logger.info(summary.getSummary(false)), 0, 5, TimeUnit.SECONDS);

        partitionerTaskPoolQueue = new LinkedBlockingQueue<>();

        AtomicInteger totalPartitions = new AtomicInteger(-1);

        ThreadFactory partitionerTaskPoolThreadFactory =
                new ThreadFactoryBuilder().setNameFormat("PartionerTaskPool-%d").build();
        partitionerTaskPool.setThreadFactory(partitionerTaskPoolThreadFactory);

        ScheduledExecutorService partitionerTaskPoolListener = Executors.newSingleThreadScheduledExecutor();

        for (Collection coll : colls) {
            PartitionTask pt = new PartitionTask(coll.getNamespace(), sourceClient,
                    destClient, partitionManager, partitionerTaskPoolQueue, retryQueue, summary, config);
            partitionerTaskPoolResults.add(partitionerTaskPool.submit(pt));
        }
        partitionerTaskPoolListener.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;
            int numPartitions = 0;

            @Override
            public void run() {
                PartitionDiffTask pt;
                while ((pt = partitionerTaskPoolQueue.poll()) != null) {
                    try {
                        if (pt == PartitionDiffTask.END_TOKEN) {
                            logger.debug("[PartitionerTaskPoolListener] saw an EndToken ({}/{})",
                                    endTokensSeen + 1, colls.size());
                            if (++endTokensSeen == colls.size()) {
                                logger.debug("[PartitionerTaskPoolListener] has seen all end tokens ({})", endTokensSeen);
                                totalPartitions.set(numPartitions);
                                summary.setTotalChunks(numPartitions);
                                partitionerTaskPoolListenerDone.set(true);
//                                break;
                            }
                        } else {
                            initialTaskPoolResults.add(initialTaskPool.submit(pt));
                            numPartitions++;
                        }
                    } catch (Exception e) {
                        logger.error("[PartitionerTaskPoolListener] Exception occurred while running partition task", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

        ThreadFactory retryTaskPoolThreadFactory = new ThreadFactoryBuilder().setNameFormat("RetryPool-%d").build();
        retryTaskPool = Executors.newFixedThreadPool(4, retryTaskPoolThreadFactory);
        List<Future<DiffResult>> retryTaskPoolResults = new CopyOnWriteArrayList<>();

        /* Once the Retryer thread has seen all the END_TOKENs this value is set.
         *  It is used as a barrier to determine when to stop the retryPool */
        AtomicInteger finalSizeRetryTaskPoolResults = new AtomicInteger(-1);

        ScheduledExecutorService retryTaskPoolListener = Executors.newSingleThreadScheduledExecutor();
        retryTaskPoolListener.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;

            @Override
            public void run() {
                PartitionRetryTask rt;
                while ((rt = (PartitionRetryTask) retryQueue.poll()) != null) {
                    try {
                        if (rt == PartitionRetryTask.END_TOKEN) {
                            logger.trace("[RetryTaskPoolListener] saw an end token ({}/{})",
                                    endTokensSeen + 1, totalPartitions.get());
                            int tp = totalPartitions.get();
                            if (++endTokensSeen == tp) {
                                logger.debug("[RetryTaskPoolListener] has seen all end tokens ({})", endTokensSeen);
                                finalSizeRetryTaskPoolResults.set(retryTaskPoolResults.size());
                                retryTaskPoolListenerDone.set(true);
                            }
                        } else {
                            logger.debug("[RetryTaskPoolListener] submitting retry {} for ({})",
                                    rt.getRetryStatus().getAttempt() + 1, rt.getChunkDef().unitString());
                            retryTaskPoolResults.add(retryTaskPool.submit(rt));
                        }
                    } catch (Exception e) {
                        logger.error("[RetryTaskPoolListener] Exception occurred while running retry task", e);
                        throw new RuntimeException(e);
                    }

                }
            }
        }, 0, 1, TimeUnit.SECONDS);

        ThreadFactory initialTaskPoolThreadFactory = new ThreadFactoryBuilder().setNameFormat("WorkerPool-%d").build();
        initialTaskPool.setThreadFactory(initialTaskPoolThreadFactory);

        ScheduledExecutorService partitionerTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
        Set<Future<Pair<String, Integer>>> partitionerTaskPoolFuturesSeen = new HashSet<>();

        Future<?> partitionerTaskPoolFuture = partitionerTaskPoolCollector.scheduleWithFixedDelay(
                new Runnable() {
                    int runs = 0;
                    int expectedResultSize = colls.size();

                    @Override
                    public void run() {
                        logger.trace("[PartitionerTaskPoolCollector] loop: {} :: {} expected results, {} seen",
                                ++runs, expectedResultSize, partitionerTaskPoolFuturesSeen.size());
                        if (partitionerTaskPoolFuturesSeen.size() < expectedResultSize) {
                            for (Future<Pair<String, Integer>> future : partitionerTaskPoolResults) {
                                try {
                                    if (!partitionerTaskPoolFuturesSeen.contains(future) && future.isDone()) {
                                        partitionerTaskPoolFuturesSeen.add(future);
                                        Pair<String, Integer> pair = future.get();
                                        int newPartitions = pair.getRight();
                                        String coll = pair.getLeft();
                                        logger.debug("[PartitionerTaskPoolCollector] saw {} partitions created for {}",
                                                newPartitions, coll);
//                                    totalPartitions.getAndAdd(newPartitions);
                                    }
                                } catch (InterruptedException e) {
                                    logger.error("[PartitionerTaskPoolCollector] was interrupted", e);
                                    throw new RuntimeException(e);
                                } catch (ExecutionException e) {
                                    logger.error("[PartitionerTaskPoolCollector] threw an exception", e);
                                    throw new RuntimeException(e);
                                }
                            }
                        } else {
                            partitionerTaskPoolCollectorDone.set(true);
                        }
                    }
                }, 0, 1000, TimeUnit.MILLISECONDS);

        // Check for futures coming off the retryPool
        Set<Future<DiffResult>> retryTaskPoolFuturesSeen = ConcurrentHashMap.newKeySet();
        ScheduledExecutorService retryTaskPoolCollector = Executors.newSingleThreadScheduledExecutor();
        Future<?> retryTaskPoolFuture = retryTaskPoolCollector.scheduleWithFixedDelay(new Runnable() {
            int runs = 0;

            @Override
            public void run() {
                int expectedRetryResults = finalSizeRetryTaskPoolResults.get();
                logger.trace("[RetryTaskPoolCollector] loop: {} :: {} expected retryResults, {} seen",
                        ++runs, expectedRetryResults, retryTaskPoolFuturesSeen.size());
//                if (expectedRetryResults >= 0) {
                if (expectedRetryResults < 0 || (expectedRetryResults >= 0 && retryTaskPoolFuturesSeen.size() < expectedRetryResults)) {
                        for (Future<DiffResult> future : retryTaskPoolResults) {
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
                                logger.error("[ResultRetryCollector] Diff task was interrupted", e);
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                logger.error("[ResultRetryCollector] Diff task threw an exception", e);
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
        Set<Future<DiffResult>> initialTaskPoolFuturesSeen = new HashSet<>();
        Future<?> initialTaskPoolFuture = initialTaskPoolCollector.scheduleWithFixedDelay(new Runnable() {
            int runs = 0;

            @Override
            public void run() {
                int expectedResultSize = totalPartitions.get();
                logger.trace("[InitialTaskPoolCollector] loop: {} :: {} expected results, {} seen",
                        ++runs, expectedResultSize, initialTaskPoolFuturesSeen.size());
                if (expectedResultSize >= 0) {
                    if (initialTaskPoolFuturesSeen.size() < expectedResultSize) {
                        for (Future<DiffResult> future : initialTaskPoolResults) {
                            try {
                                if (!initialTaskPoolFuturesSeen.contains(future) && future.isDone()) {
                                    initialTaskPoolFuturesSeen.add(future);
                                    DiffResult result = future.get();
                                    int failures = result.getFailedKeys().size();
                                    logger.trace("[InitialTaskPoolCollector] got result for {}: " +
                                                    "{} matches, {} failures, {} bytes",
                                            result.getChunkDef().unitString(), result.getMatches(),
                                            failures, result.getBytesProcessed());

                                    summary.updateInitTask(result);

                                }
                            } catch (InterruptedException e) {
                                logger.error("[InitialTaskPoolCollector] diff task was interrupted", e);
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                logger.error("[InitialTaskPoolCollector] diff task threw an exception", e);
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        initialTaskPoolCollectorDone.set(true);
                    }
                }
            }
        }, 0, 1, TimeUnit.SECONDS);


        boolean partitionerTaskPoolResult = false;
        boolean initialTaskPoolResult = false;
        boolean retryTaskPoolResult = false;
        while (!(partitionerTaskPoolDone.get() && initialTaskPoolDone.get() && retryTaskPoolDone.get())) {
            try {
                Thread.sleep(1000);
                logger.trace("Check completion status");
                if (!partitionerTaskPoolDone.get()) {
                    if (partitionerTaskPoolListenerDone.get() && !partitionerTaskPoolListener.isShutdown()) {
                        logger.info("[Main] shutting down partitioner task pool listener");
                        partitionerTaskPoolListener.shutdown();
                    } else {
                        logger.trace("[Main] task pool listener still running");
                    }
                    if (partitionerTaskPoolCollectorDone.get() && !partitionerTaskPoolCollector.isShutdown()) {
                        logger.info("[Main] shutting down partitioner task pool collector");
                        partitionerTaskPoolResult = partitionerTaskPoolFuture.cancel(false);
                        partitionerTaskPoolCollector.shutdown();
                    } else {
                        logger.trace("[Main] partitioner task pool collector still running");
                    }
                    if (partitionerTaskPoolResult) {
                        partitionerTaskPoolDone.set(true);
                        logger.info("[Main] shutting down partitioner task pool");
                        partitionerTaskPool.shutdown();
                        if (!partitionerTaskPoolListener.isShutdown()) {
                            partitionerTaskPoolListener.shutdown();
                        }
                    }
                } else {
                    logger.trace("[Main] partitioner task pool still running");
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
                        logger.info("[Main] shutting down initial task pool");
                        initialTaskPool.shutdown();
                    }
                } else {
                    logger.trace("[Main] Initial task pool still runnign");
                }

                if (!retryTaskPoolDone.get()) {
                    if (retryTaskPoolListenerDone.get() && !retryTaskPoolListener.isShutdown()) {
                        logger.info("[Main] shutting down retry task pool listener");
                        retryTaskPoolListener.shutdown();
                    } else {
                        logger.trace("[Main] Retry pool listener still running");
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
                    logger.trace("[Main] Retry pool still running");
                }
            } catch (Exception e) {
                logger.error("[Main] Error collecting partition pool and/or worker pool", e);
                throw new RuntimeException(e);
            }
        }

        logger.info("[Main] shutting down statusReporter thread");
        statusReporter.shutdown();

        logger.info(summary.getSummary(true));
        sourceClient.close();
        destClient.close();
    }

    private MongoClient initClient(String uri) {
        ConnectionString connStr = new ConnectionString(uri);
        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(connStr)
                .uuidRepresentation(UuidRepresentation.STANDARD).build();
        return MongoClients.create(mcs);
    }


}
