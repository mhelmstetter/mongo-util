package com.mongodb.diff3;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.util.BlockWhenQueueFull;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class DifferentDiffUtil {
    private static Logger logger = LoggerFactory.getLogger(DifferentDiffUtil.class);
    private MongoClient sourceClient;
    private MongoClient destClient;
    private DatabaseCatalogProvider databaseCatalogProvider;
    private PartitionManager partitionManager;
    private final DiffConfiguration config;
    private ThreadPoolExecutor executor;
    private List<Future<PartitionDiffResult>> diffResults = new ArrayList<>();
    private long estimatedTotalDocs;
    private long totalSize;

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

        int queueSize = colls.size();
        logger.debug("Setting queue size to {}", queueSize);
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(queueSize);
        int numThreads = config.getThreads();
        executor = new ThreadPoolExecutor(numThreads, numThreads, 30,
                TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
    }

    public void run() {
        Set<Collection> colls = databaseCatalogProvider.get().getUnshardedCollections();
        DiffSummary summary = new DiffSummary(colls.size(), estimatedTotalDocs, totalSize);

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info(summary.getSummary(false));
            }
        }, 0, 5, TimeUnit.SECONDS);

        for (Collection coll : colls) {
            PartitionDiffTask task = new PartitionDiffTask(
                    coll.getNamespace(), sourceClient, destClient, partitionManager);
            diffResults.add(executor.submit(task));
        }

        Set<Future<PartitionDiffResult>> futuresSeen = new HashSet<>();
        while (futuresSeen.size() < diffResults.size()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            for (Future<PartitionDiffResult> future : diffResults) {
                try {
                    if (!futuresSeen.contains(future) && future.isDone()) {
                        futuresSeen.add(future);
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
        }

        statusReporter.shutdown();
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
