package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
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
    BlockingQueue<Runnable> workQueue;
    
    protected ThreadPoolExecutor executor = null;
    private ExecutorCompletionService<DupeIdTaskResult> completionService;
    
    long count = 0;
    long dupeCount = 0;
    long duplicateDocsCount = 0;
    int completedTaskCount = 0;
    AtomicInteger submitCount = new AtomicInteger(0);

    public DupeIdCollectionWorker(MongoCollection<RawBsonDocument> collection, MongoDatabase archiveDb, int threads) {
        this.collection = collection;
        this.archiveDb = archiveDb;
        
        workQueue = new ArrayBlockingQueue<>(queueSize);
        executor = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue, new CallerBlocksPolicy(ONE_MINUTE * 5));
        completionService = new ExecutorCompletionService<>(executor);
    }
    
    @Override
    public void run() {
        
        long start = System.currentTimeMillis();
        

        try {
            
            logger.debug("starting worker query {}", collection.getNamespace());
            long total = collection.estimatedDocumentCount();
            
            int sampleCount = 25000;
            
            if (total < 100000) {
            	sampleCount = (int)Math.round(total * 0.01);
            }
            logger.debug("{} estimated doc count: {}, sample count: {}", collection.getNamespace(), total, sampleCount);
            
            List<Bson> pipeline = new ArrayList<>();
            pipeline.add(Aggregates.project(Projections.fields(Projections.include("_id"))));
            pipeline.add(Aggregates.sample(sampleCount));
            pipeline.add(Aggregates.sort(Sorts.orderBy(Sorts.ascending("_id"))));
            
            AggregateIterable<RawBsonDocument> results = collection.aggregate(pipeline);
            
            BsonValue last = null;
            int taskNum = 1 ;
            for (BsonDocument result : results) {
                BsonValue id = result.get("_id");
                DupeIdTask task = new DupeIdTask(collection, archiveDb, last, id);
                
                completionService.submit(task);
                submitCount.incrementAndGet();
                last = id;
            }
            
            // Add an additional task to cover the range after the last sampled _id
            if (last != null) {
                DupeIdTask finalTask = new DupeIdTask(collection, archiveDb, last, null);
                completionService.submit(finalTask);
                submitCount.incrementAndGet();
            }
            
            logger.debug("submitted {} tasks", submitCount.get());

            // Create a thread to log progress every 30 seconds
            Thread progressLogger = new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(LOG_INTERVAL);
                        double completionPercentage = (completedTaskCount / (double) submitCount.get()) * 100;
                        
                        logger.info(String.format("(%.2f%%) processed %,d documents, found %,d duplicates, %,d documents having duplicate _ids", 
                                completionPercentage, count, dupeCount, duplicateDocsCount));
                        

                    }
                } catch (InterruptedException e) {
                    // Exit when interrupted
                }
            });
            progressLogger.start();
            
            for (int i = 0; i < submitCount.get(); i++) {
                DupeIdTaskResult result = completionService.take().get();
                dupeCount += result.dupeCount;
                count += result.totalCount;
                duplicateDocsCount += result.duplicateDocsCount;
                completedTaskCount ++;
            }
            
            // Interrupt the logging thread after processing
            progressLogger.interrupt();
            
        } catch (Exception e) {
            logger.error("worker run error", e);
        } finally {
            
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start) / 1000.0;
        logger.debug(String.format("Done dupe _id check %s, %s documents in %f seconds, dupe id count: %s, dupe docs count: %s",
                collection.getNamespace(), count, dur, dupeCount, duplicateDocsCount));
        
        shutdown();

    }
    
	public void shutdown() {
		logger.debug("DupeIdCollectionWorker starting shutdown");
		executor.shutdown();
		try {
			Thread.sleep(10000);
			executor.awaitTermination(999, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			logger.warn("DupeIdCollectionWorker interrupted");
			Thread.currentThread().interrupt();
		}
		logger.debug("DupeIdCollectionWorker shutdown complete");
	}
}
