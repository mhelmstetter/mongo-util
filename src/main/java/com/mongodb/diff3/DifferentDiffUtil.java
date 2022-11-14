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
    private List<Future<PartitionDiffResult>> diffResults = new ArrayList<>();
    private List<Future<Pair<String, Integer>>> partitionResults = new ArrayList<>();
    private long estimatedTotalDocs;
    private long totalSize;
    private Queue<PartitionDiffTask> partitionTaskQueue;

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
        logger.info("Collections: [" + String.join(", ", colls) + "]");

        int queueSize = colls.size() * 8;
        logger.debug("Setting queue size to {}", queueSize);
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
                    destClient, partitionManager, partitionTaskQueue);
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

        ThreadFactory workerThreadFactory = new ThreadFactoryBuilder().setNameFormat("WorkerPool-%d").build();
        executor.setThreadFactory(workerThreadFactory);

        ScheduledExecutorService partitionCollector = Executors.newSingleThreadScheduledExecutor();
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
                    logger.debug("[PartitionCollector] loop: {} :: {} expected results, {} seen",
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
                    logger.debug("[PartitionDiffCollector] loop: {} :: {} expected results, {} seen",
                            ++runs, expectedResultSize, pdFuturesSeen.size());
                    if (expectedResultSize >= 0) {
                        if (pdFuturesSeen.size() < expectedResultSize) {
                            for (Future<PartitionDiffResult> future : diffResults) {
                                try {
                                    if (!pdFuturesSeen.contains(future) && future.isDone()) {
                                        pdFuturesSeen.add(future);
                                        PartitionDiffResult result = future.get();
                                        int failures = result.getFailureCount();
                                        logger.debug("Got result for {}: {} matches, {} failures, {} bytes",
                                                result.namespace, result.matches, failures, result.bytesProcessed);

                                        if (failures > 0) {
                                            summary.incrementFailedChunks(1);
                                            summary.incrementSuccessfulDocs(result.matches - failures);
                                        } else {
                                            summary.incrementSuccessfulChunks(1);
                                            summary.incrementSuccessfulDocs(result.matches);
                                        }
                                        summary.incrementProcessedDocs(result.matches + failures);
                                        summary.incrementFailedDocs(failures);
                                        summary.incrementProcessedChunks(1);
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

        boolean partitionPoolDone = false;
        boolean workerPoolDone = false;

        while (!(partitionPoolDone && workerPoolDone)) {
            try {
                Thread.sleep(100);

                if (!partitionPoolDone && partitionFuture.isDone()) {
                    partitionFuture.get();
                    logger.info("[Main] partition pool is Done");
                    partitionPoolDone = true;
                }
                if (!workerPoolDone && partitionDiffFuture.isDone()) {
                    partitionDiffFuture.get();
                    logger.info("[Main] worker pool is done");
                    workerPoolDone = true;
                }
            } catch (Exception e) {
                logger.error("[Main] Error collecting partition pool and/or worker pool", e);
                throw new RuntimeException(e);
            }
        }


        statusReporter.shutdown();
        partitioner.shutdown();
        partitionListener.shutdown();
        partitionCollector.shutdown();
        partitionDiffCollector.shutdown();
        executor.shutdown();
        logger.info(summary.getSummary(true));
    }

    private MongoClient initClient(String uri) {
        ConnectionString connStr = new ConnectionString(uri);
        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(connStr)
                .uuidRepresentation(UuidRepresentation.STANDARD).build();
        return MongoClients.create(mcs);
    }


}
