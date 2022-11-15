package com.mongodb.diff3;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.util.BlockWhenQueueFull;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DifferentDiffUtil {
    private static Logger logger = LoggerFactory.getLogger(DifferentDiffUtil.class);
    private MongoClient sourceClient;
    private MongoClient destClient;
    private DatabaseCatalogProvider databaseCatalogProvider;
    private PartitionManager partitionManager;
    private final DiffConfiguration config;
    private ThreadPoolExecutor executor;

    private ThreadPoolExecutor partitioner;
    private ExecutorService retryPool;
    private List<Future<PartitionDiffResult>> diffResults = new ArrayList<>();
    private List<Future<Pair<String, Integer>>> partitionResults = new ArrayList<>();
    private long estimatedTotalDocs;
    private long totalSize;
    private Queue<PartitionDiffTask> partitionTaskQueue;
    private Queue<PartitionRetryTask> retryQueue;

    public DifferentDiffUtil(DiffConfiguration config) {
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
        executor = new ThreadPoolExecutor(numThreads, numThreads, 30,
                TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
        partitioner = new ThreadPoolExecutor(Math.max(colls.size(), 16), Math.max(colls.size(), 16), 30,
                TimeUnit.SECONDS, partitionerWorkQueue, new BlockWhenQueueFull());
    }

    public void run() {
        Set<Collection> colls = databaseCatalogProvider.get().getUnshardedCollections();
        DiffSummary summary = new DiffSummary(estimatedTotalDocs, totalSize);
        retryQueue = new LinkedBlockingQueue<>();

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info(summary.getSummary(false));
            }
        }, 0, 5, TimeUnit.SECONDS);

        partitionTaskQueue = new LinkedBlockingQueue<>();

        AtomicInteger totalPartitions = new AtomicInteger(-1);

        ThreadFactory partitionerThreadFactory = new ThreadFactoryBuilder().setNameFormat("PartionerPool-%d").build();
        partitioner.setThreadFactory(partitionerThreadFactory);

        ScheduledExecutorService partitionListener = Executors.newSingleThreadScheduledExecutor();

        for (Collection coll : colls) {
            PartitionTask pt = new PartitionTask(coll.getNamespace(), sourceClient,
                    destClient, partitionManager, partitionTaskQueue, retryQueue);
            partitionResults.add(partitioner.submit(pt));
        }
        partitionListener.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;
            int numPartitions = 0;

            @Override
            public void run() {
                PartitionDiffTask pt;
                while ((pt = partitionTaskQueue.poll()) != null) {
                    if (pt != null) {
                        try {
                            if (pt == PartitionDiffTask.END_TOKEN) {
                                logger.debug("[PartitionListener] saw an EndToken ({}/{})",
                                        endTokensSeen + 1, colls.size());
                                if (++endTokensSeen == colls.size()) {
                                    logger.debug("[PartitionListener] has seen all end tokens ({})", endTokensSeen);
                                    totalPartitions.set(numPartitions);
                                    summary.setTotalChunks(numPartitions);
                                    break;
                                }
                            } else {
                                diffResults.add(executor.submit(pt));
                                numPartitions++;
                            }
                        } catch (Exception e) {
                            logger.error("[PartitionListener] Exception occurred while running partition task", e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }, 0, 100, TimeUnit.MILLISECONDS);

        ThreadFactory retryThreadFactory = new ThreadFactoryBuilder().setNameFormat("RetryPool-%d").build();
        retryPool = Executors.newFixedThreadPool(4, retryThreadFactory);
        List<Future<PartitionDiffResult>> retryResults = new CopyOnWriteArrayList<>();

        /* Once the Retryer thread has seen all the END_TOKENs this value is set.
         *  It is used as a barrier to determine when to stop the retryPool */
        AtomicInteger finalSizeRetryResults = new AtomicInteger(-1);

        ScheduledExecutorService failedTaskListener = Executors.newSingleThreadScheduledExecutor();
        failedTaskListener.scheduleWithFixedDelay(new Runnable() {
            int endTokensSeen = 0;

            @Override
            public void run() {
                PartitionRetryTask rt;
                while ((rt = retryQueue.poll()) != null) {
                    if (rt != null) {
                        try {
                            if (rt == PartitionRetryTask.END_TOKEN) {
                                logger.debug("[FailedTaskListener] saw an end token ({}/{})",
                                        endTokensSeen + 1, totalPartitions.get());
                                int tp = totalPartitions.get();
                                if (++endTokensSeen == tp) {
                                    logger.debug("[FailedTaskListener] has seen all end tokens ({})", endTokensSeen);
                                    finalSizeRetryResults.set(retryResults.size());
                                }
                            } else {
                                PartitionDiffTask originalTask = rt.getOriginalTask();
                                logger.debug("[FailedTaskListener] submitting retry {} for ({})",
                                        rt.getRetryStatus().getAttempt() + 1, originalTask.getPartition().toString());
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

        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("WorkerPool-%d").build();
        executor.setThreadFactory(workerThreadFactory);

        ExecutorService partitionCollector = Executors.newSingleThreadExecutor();
        Set<Future<Pair<String, Integer>>> pFuturesSeen = new HashSet<>();

        Future<?> partitionFuture = partitionCollector.submit(new Runnable() {
            @Override
            public void run() {
                int runs = 0;
                while (true) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    int expectedResultSize = colls.size();
                    logger.trace("[PartitionCollector] loop: {} :: {} expected results, {} seen",
                            ++runs, expectedResultSize, pFuturesSeen.size());
                    if (pFuturesSeen.size() < expectedResultSize) {
                        for (Future<Pair<String, Integer>> future : partitionResults) {
                            try {
                                if (!pFuturesSeen.contains(future) && future.isDone()) {
                                    pFuturesSeen.add(future);
                                    Pair<String, Integer> pair = future.get();
                                    int newPartitions = pair.getRight();
                                    String coll = pair.getLeft();
                                    logger.debug("[PartitionCollector] saw {} partitions created for {}",
                                            newPartitions, coll);
//                                    totalPartitions.getAndAdd(newPartitions);
                                }
                            } catch (InterruptedException e) {
                                logger.error("[PartitionCollector] was interrupted", e);
                                throw new RuntimeException(e);
                            } catch (ExecutionException e) {
                                logger.error("[PartitionCollector] threw an exception", e);
                                throw new RuntimeException(e);
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        });

        // Check for futures coming off the retryPool
        Set<Future<PartitionDiffResult>> retriesSeen = ConcurrentHashMap.newKeySet();
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
                            for (Future<PartitionDiffResult> future : retryResults) {
                                try {
                                    if (!retriesSeen.contains(future) && future.isDone()) {
                                        retriesSeen.add(future);
                                        PartitionDiffResult result = future.get();
                                        int failures = result.getFailureCount();

                                        if (failures > 0 && result.isRetryable()) {
                                            // There's failures but will retry
                                            logger.trace("[ResultRetryCollector] ignoring retried result for ({}): " +
                                                            "{} matches, {} failures, {} bytes",
                                                    result.getNs(), result.getPartition().toString(),
                                                    result.getFailureCount(), result.getBytesProcessed());
                                            continue;
                                        }

                                        logger.debug("[ResultRetryCollector] got final result for ({}): " +
                                                        "{} matches, {} failures, {} bytes",
                                                result.getPartition().toString(), result.getMatches(),
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

        ScheduledExecutorService partitionDiffCollector = Executors.newSingleThreadScheduledExecutor();
        Set<Future<PartitionDiffResult>> pdFuturesSeen = new HashSet<>();
        Future<?> partitionDiffFuture = partitionDiffCollector.submit(new Runnable() {
            @Override
            public void run() {
                int runs = 0;
                while (true) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    int expectedResultSize = totalPartitions.get();
                    logger.trace("[PartitionDiffCollector] loop: {} :: {} expected results, {} seen",
                            ++runs, expectedResultSize, pdFuturesSeen.size());
                    if (expectedResultSize >= 0) {
                        if (pdFuturesSeen.size() < expectedResultSize) {
                            for (Future<PartitionDiffResult> future : diffResults) {
                                try {
                                    if (!pdFuturesSeen.contains(future) && future.isDone()) {
                                        pdFuturesSeen.add(future);
                                        PartitionDiffResult result = future.get();
                                        int failures = result.getFailureCount();
                                        logger.debug("[PartitionDiffCollector] got result for {}: " +
                                                        "{} matches, {} failures, {} bytes",
                                                result.getPartition().toString(), result.getMatches(),
                                                failures, result.getBytesProcessed());

                                        summary.updateInitTask(result);

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

        boolean partitionPoolDone = false;
        boolean workerPoolDone = false;
        boolean retryPoolDone = false;

        while (!(partitionPoolDone && workerPoolDone && retryPoolDone)) {
            try {
                Thread.sleep(100);

                if (!partitionPoolDone && partitionFuture.isDone()) {
                    partitionFuture.get();
                    logger.info("[Main] partition pool is Done");
                    partitionPoolDone = true;

                    logger.info("[Main] shutting down partitioner thread pool");
                    partitioner.shutdown();

                    logger.info("[Main] shutting down partition listener thread");
                    partitionListener.shutdown();

                    logger.info("[Main] shutting down partition collector thread");
                    partitionCollector.shutdown();
                }
                if (!workerPoolDone && partitionDiffFuture.isDone()) {
                    partitionDiffFuture.get();
                    logger.info("[Main] worker pool is done");
                    workerPoolDone = true;
                }
                if (!retryPoolDone && retryPoolResultFuture.isDone()) {
                    retryPoolResultFuture.get();
                    logger.info("[Main] retry pool is done");
                    retryPoolDone = true;
                }
            } catch (Exception e) {
                logger.error("[Main] Error collecting partition pool and/or worker pool", e);
                throw new RuntimeException(e);
            }
        }

        logger.info("[Main] shutting down statusReporter thread");
        statusReporter.shutdown();

        logger.info("[Main] shutting down diff collector thread");
        partitionDiffCollector.shutdown();

        logger.info("[Main] shutting down failed task listener thread");
        failedTaskListener.shutdown();

        logger.info("[Main] shutting down retry collector thread");
        retryResultCollector.shutdown();

        logger.info("[Main] shutting down retry thread pool");
        retryPool.shutdown();

        logger.info("[Main] shutting down worker diff pool");
        executor.shutdown();
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
