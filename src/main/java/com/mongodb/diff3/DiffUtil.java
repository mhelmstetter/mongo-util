package com.mongodb.diff3;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.mongodb.model.Collection;
import com.mongodb.shardsync.ChunkManager;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.DatabaseCatalog;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class DiffUtil {

    private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);
    private List<String> destShardNames;
    private List<String> srcShardNames;

    private ShardClient sourceShardClient;
    private ShardClient destShardClient;

    private DiffConfiguration config;

    protected Map<String, ThreadPoolExecutor> executorMap = new HashMap<>();
    Map<String, List<Future<DiffResult>>> diffResultMap = new HashMap<>();

    private Map<String, Map<String, RawBsonDocument>> sourceChunksCacheMap = new HashMap<>();
    private long estimatedTotalDocs;
    private long totalSize;


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
            logger.debug("Setting workQueue size to {}", qSize);
            BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(qSize);
            diffResultMap.put(shard, new ArrayList<>(chunkMap.size()));
            executorMap.put(shard, new ThreadPoolExecutor(numThreads, numThreads,
                    30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull()));
        }
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
        DiffSummary summary = new DiffSummary(estimatedTotalDocs, totalSize);

        ScheduledExecutorService statusReporter = Executors.newSingleThreadScheduledExecutor();
        statusReporter.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                logger.info(summary.getSummary(false));
            }
        }, 0, 5, TimeUnit.SECONDS);

        for (int i = 0; i < srcShardNames.size(); i++) {
            String srcShard = srcShardNames.get(i);
            String destShard = destShardNames.get(i);
            Map<String, RawBsonDocument> chunkCache = sourceChunksCacheMap.get(srcShard);
            for (RawBsonDocument chunk : chunkCache.values()) {
                ShardedDiffTask task = new ShardedDiffTask(sourceShardClient, destShardClient,
                        config, chunk, srcShard, destShard);
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
                    unshardedColl.getNamespace().getNamespace(), srcShard, destShard);
            logger.debug("Added an UnshardedDiffTask for {}--{}",
                    unshardedColl.getNamespace(), srcShard);
            List<Future<DiffResult>> diffResults = diffResultMap.get(srcShard);
            ThreadPoolExecutor executor = executorMap.get(srcShard);
            diffResults.add(executor.submit(task));
        }

        Set<String> finishedShards = new HashSet<>();
        Map<String, Set<Future<DiffResult>>> futSeenMap = new HashMap<>();

        // Poll for completed futures every second
        while (finishedShards.size() < numShards) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
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
                                if (result instanceof UnshardedDiffResult) {
                                    UnshardedDiffResult udr = (UnshardedDiffResult) result;
                                    logger.debug("Got unsharded result for {}--{}: {} matches, {} failures, {} bytes",
                                            udr.getNs(), shard, udr.matches, udr.getFailureCount(), udr.bytesProcessed);
                                } else if (result instanceof ShardedDiffResult) {
                                    ShardedDiffResult sdr = (ShardedDiffResult) result;
                                    logger.debug("Got sharded result for {}--{}: {} matches, {} failures, {} bytes",
                                            sdr.getNs(), shard, sdr.matches, sdr.getFailureCount(),
                                            sdr.bytesProcessed);
                                }
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
        statusReporter.shutdown();

        for (ThreadPoolExecutor executor : executorMap.values()) {
            executor.shutdown();
        }
        logger.info(summary.getSummary(true));
    }

}
