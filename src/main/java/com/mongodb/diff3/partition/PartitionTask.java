package com.mongodb.diff3.partition;

import com.mongodb.client.MongoClient;
import com.mongodb.diff3.DiffConfiguration;
import com.mongodb.diff3.DiffSummary;
import com.mongodb.diff3.RetryTask;
import com.mongodb.model.Namespace;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;

public class PartitionTask implements Callable<Pair<String, Integer>> {
    private final Namespace namespace;
    private final MongoClient sourceClient;
    private final MongoClient destClient;
    private final PartitionManager partitionManager;
    private final Queue<PartitionDiffTask> partitionQueue;
    private final Queue<RetryTask> retryQueue;
    private final DiffSummary summary;
    private final DiffConfiguration config;

    private  static final Logger logger = LoggerFactory.getLogger(PartitionTask.class);

    public PartitionTask(Namespace namespace, MongoClient sourceClient, MongoClient destClient,
                         PartitionManager partitionManager, Queue<PartitionDiffTask> partitionQueue,
                         Queue<RetryTask> retryQueue, DiffSummary summary, DiffConfiguration config) {
        this.namespace = namespace;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.partitionManager = partitionManager;
        this.partitionQueue = partitionQueue;
        this.retryQueue = retryQueue;
        this.summary = summary;
        this.config = config;
    }

    @Override
    public Pair<String, Integer> call() throws Exception {
        long start = System.currentTimeMillis();

        List<Partition> partitions = partitionManager.partitionCollection(namespace, sourceClient);
        logger.debug("[{}] created {} partitions for {}",
                Thread.currentThread().getName(), partitions.size(), namespace.getNamespace());
        Collections.shuffle(partitions);

        for (Partition p : partitions) {
            logger.debug("[{}] added {} to the partition queue", Thread.currentThread().getName(), p.toString());
            partitionQueue.add(new PartitionDiffTask(p, sourceClient, destClient, retryQueue, summary, config));
        }
        partitionQueue.add(PartitionDiffTask.END_TOKEN);
        logger.debug("[{}] Partition task completed in {} ms",
                Thread.currentThread().getName(), System.currentTimeMillis() - start);
        return Pair.of(namespace.getNamespace(), partitions.size());
    }
}
