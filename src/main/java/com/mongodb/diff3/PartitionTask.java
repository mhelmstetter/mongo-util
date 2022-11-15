package com.mongodb.diff3;

import com.mongodb.client.MongoClient;
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
    private final Queue<PartitionRetryTask> retryQueue;

    private  static final Logger logger = LoggerFactory.getLogger(PartitionTask.class);
    private long start;

    public PartitionTask(Namespace namespace, MongoClient sourceClient, MongoClient destClient,
                         PartitionManager partitionManager, Queue<PartitionDiffTask> partitionQueue,
                         Queue<PartitionRetryTask> retryQueue) {
        this.namespace = namespace;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.partitionManager = partitionManager;
        this.partitionQueue = partitionQueue;
        this.retryQueue = retryQueue;
    }

    @Override
    public Pair<String, Integer> call() throws Exception {
        start = System.currentTimeMillis();

        List<Partition> partitions = partitionManager.partitionCollection(namespace, sourceClient);
        logger.debug("[{}] created {} partitions for {}",
                Thread.currentThread().getName(), partitions.size(), namespace.getNamespace());
        Collections.shuffle(partitions);

        for (Partition p : partitions) {
            logger.debug("[{}] added {} to the partition queue", Thread.currentThread().getName(), p.toString());
            partitionQueue.add(new PartitionDiffTask(p, sourceClient, destClient, retryQueue));
        }
        partitionQueue.add(PartitionDiffTask.END_TOKEN);
        logger.debug("[{}] Partition task completed in {} ms",
                Thread.currentThread().getName(), System.currentTimeMillis() - start);
        return Pair.of(namespace.getNamespace(), partitions.size());
    }
}
