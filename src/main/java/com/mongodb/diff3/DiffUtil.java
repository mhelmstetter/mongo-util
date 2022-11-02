package com.mongodb.diff3;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.mongodb.model.Collection;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.DatabaseCatalog;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class DiffUtil {

    private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);


    private ShardClient sourceShardClient;
    private ShardClient destShardClient;

    private DiffConfiguration config;

    protected ThreadPoolExecutor executor = null;
    private BlockingQueue<Runnable> workQueue;
    List<Future<DiffResult>> diffResults;

    private Map<String, RawBsonDocument> sourceChunksCache;
    private Set<String> chunkCollSet;
    private long estimatedTotalDocs;
    private long totalSize;


    public DiffUtil(DiffConfiguration config) {
        this.config = config;

        sourceShardClient = new ShardClient("source", config.getSourceClusterUri());
        destShardClient = new ShardClient("dest", config.getDestClusterUri());

        sourceShardClient.init();
        destShardClient.init();
        
        sourceShardClient.populateCollectionsMap();
        DatabaseCatalog catalog = sourceShardClient.getDatabaseCatalog();
        
        long[] sizeAndCount = catalog.getTotalSizeAndCount();
        totalSize = sizeAndCount[0];
        estimatedTotalDocs = sizeAndCount[1];

        Set<String> shardedColls = catalog.getShardedCollections().stream()
                .map(c -> c.getNamespace()).collect(Collectors.toSet());
        Set<String> unshardedColls = catalog.getUnshardedCollections().stream()
                .map(c -> c.getNamespace()).collect(Collectors.toSet());

        logger.info("ShardedColls:[" + String.join(", ", shardedColls) + "]");

        logger.info("UnshardedColls:[" + String.join(", ", unshardedColls) + "]");
        sourceShardClient.populateCollectionsMap();
        sourceChunksCache = sourceShardClient.loadChunksCache(config.getChunkQuery());

        workQueue = new ArrayBlockingQueue<Runnable>(sourceChunksCache.size());
        diffResults = new ArrayList<>(sourceChunksCache.size());
        executor = new ThreadPoolExecutor(config.getThreads(), config.getThreads(), 30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
    }
    

    public void run() {
        DiffSummary summary = new DiffSummary(sourceChunksCache.size(), estimatedTotalDocs, totalSize);
        for (RawBsonDocument chunk : sourceChunksCache.values()) {
            ShardedDiffTask task = new ShardedDiffTask(sourceShardClient, destShardClient, config, chunk);
            diffResults.add(executor.submit(task));
        }
        
        // TODO iterate all non-sharded namespaces and create tasks for those also
        // Make DiffTask an abstract base class? ShardedDiffTask / UnShardedDiffTask?
        for (Collection unshardedColl : sourceShardClient.getDatabaseCatalog().getUnshardedCollections()) {
            UnshardedDiffTask task = new UnshardedDiffTask(sourceShardClient, destShardClient, unshardedColl.getNamespace());
            diffResults.add(executor.submit(task));
        }

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info(summary.getSummary(false));
            }
        }, 0, 5, TimeUnit.SECONDS);


        executor.shutdown();

        for (Future<DiffResult> future : diffResults) {
            try {
                DiffResult result = future.get();
                int failures = result.getFailureCount();

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


                summary.incrementProcessedDocs(result.matches + failures);
                summary.incrementFailedDocs(failures);
                if (result instanceof ShardedDiffResult) {
                    summary.incrementProcessedChunks(1);
                }
                summary.incrementSourceOnly(result.onlyOnSource);
                summary.incrementDestOnly(result.onlyOnDest);
                summary.incrementProcessedSize(result.bytesProcessed);

                //logger.debug("result: {}", );
            } catch (InterruptedException e) {
                logger.error("Diff task was interrupted", e);
            } catch (ExecutionException e) {
                logger.error("Diff task threw an exception", e);
            }
        }
        statusReporter.shutdown();
        logger.info(summary.getSummary(true));
    }


}
