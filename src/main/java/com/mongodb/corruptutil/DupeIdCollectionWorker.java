package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.util.CallerBlocksPolicy;

public class DupeIdCollectionWorker implements Runnable {
    
    private final static int ONE_MINUTE = 60 * 1000;
    private final static int LOG_INTERVAL = 30 * 1000; // 30 seconds

    private static Logger logger = LoggerFactory.getLogger(DupeIdCollectionWorker.class);

    private MongoCollection<RawBsonDocument> collection;
    
    private MongoDatabase archiveDb;
    
    
    Bson sort = eq("_id", 1);
    
    private final int queueSize = 25000;
    private final int threads = 4;
    BlockingQueue<Runnable> workQueue;
    
    protected ThreadPoolExecutor executor = null;
    private ExecutorCompletionService<DupeIdTaskResult> completionService;
    
    long count = 0;
    long dupeCount = 0;

    public DupeIdCollectionWorker(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb) {
        this.collection = collection;
        this.archiveDb = archiveDb;
        
        workQueue = new ArrayBlockingQueue<>(queueSize);
        executor = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue, new CallerBlocksPolicy(ONE_MINUTE * 5));
        completionService = new ExecutorCompletionService<>(executor);
    }
    
    @Override
    public void run() {
        
        long start = System.currentTimeMillis();
        
        long submitCount = 0;

        try {
            
            logger.debug("starting worker query {}", collection.getNamespace());
            Number total = collection.estimatedDocumentCount();
            
            logger.debug("{} estimated doc count: {}", collection.getNamespace(), total);
            
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(Aggregates.project(Projections.fields(Projections.include("_id"))));
            pipeline.add(Aggregates.sample(25000));
            pipeline.add(Aggregates.sort(Sorts.orderBy(Sorts.ascending("_id"))));
            
            AggregateIterable<RawBsonDocument> results = collection.aggregate(pipeline);
            
            BsonValue last = null;
            for (BsonDocument result : results) {
                BsonValue id = result.get("_id");
                DupeIdTask task = new DupeIdTask(collection, archiveDb, last, id);
                completionService.submit(task);
                submitCount++;
                last = id;
            }
            logger.debug("submitted {} tasks", submitCount);

            // Create a thread to log progress every 30 seconds
            Thread progressLogger = new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(LOG_INTERVAL);
                        logger.info("Current status: processed {} documents, found {} duplicates", count, dupeCount);
                    }
                } catch (InterruptedException e) {
                    // Exit when interrupted
                }
            });
            progressLogger.start();
            
            for (int i = 0; i < submitCount; i++) {
                DupeIdTaskResult result = completionService.take().get();
                dupeCount += result.dupeCount;
                count += result.totalCount;
            }
            
            // Interrupt the logging thread after processing
            progressLogger.interrupt();
            
        } catch (Exception e) {
            logger.error("worker run error", e);
        } finally {
            
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start) / 1000.0;
        logger.debug(String.format("Done dupe _id check %s, %s documents in %f seconds, dupe id count: %s",
                collection.getNamespace(), count, dur, dupeCount));

    }
}
