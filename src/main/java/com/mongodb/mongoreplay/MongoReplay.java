package com.mongodb.mongoreplay;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.DataFormatException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class MongoReplay extends AbstractMongoReplayUtil {

    private Map<AccumulatorKey, DescriptiveStatistics> accumulators = new HashMap<AccumulatorKey, DescriptiveStatistics>();

    private int NANOS_TO_MILLIS = 1000000;

    public void execute() throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, FileNotFoundException, DataFormatException {
        init();
        for (String filename : fileNames) {
            replayFile(filename);
        }

        logger.debug("Processing results");
        for (Future<ReplayResult> future : futures) {
            ReplayResult result = null;
            try {
                result = future.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Error getting future", e);
            }
            
            if (result == null) {
                continue;
            }
            

            AccumulatorKey key = new AccumulatorKey(result.getDbName(), result.getCollectionName(), result.getCommand(), result.getQueryShape());
            DescriptiveStatistics stats = accumulators.get(key);
            if (stats == null) {
                stats = new DescriptiveStatistics();
                accumulators.put(key, stats);
            }
            stats.addValue(result.getDuration());

        }
        logger.debug("DONE processing results");
        if (pool.isPaused()) {
            logger.debug("pool is paused, resuming");
            pool.resume();
        }
        close();

        int maxNamespaceLen = 0;
        int maxShapeLen = 0;
        for (AccumulatorKey key : accumulators.keySet()) {
            int nsLen = key.getNamespace().length();
            if (nsLen > maxNamespaceLen) {
                maxNamespaceLen = nsLen;
            }
            if (key.getShape() != null) {
                int shapeLen = key.getShape().length();
                if (shapeLen > maxShapeLen) {
                    maxShapeLen = shapeLen;
                }
            }
            
        }
        maxNamespaceLen++;
        maxShapeLen++;
        
        String formatString = "%-" + maxNamespaceLen + "s %-" + maxShapeLen +  "s %-10s %-10d %-10.0f %-10.0f %-10.0f %-10.0f";
        String headerFormatString = "%-" + maxNamespaceLen + "s %-" + maxShapeLen +  "s %-10s %-10s %-10s %-10s %-10s %-10s";
        System.out.println(String.format(headerFormatString, "namespace", "query shape", "cmd", "count", "min", "max", "avg", "95p"));

        for (Map.Entry<AccumulatorKey, DescriptiveStatistics> entry : accumulators.entrySet()) {
            AccumulatorKey key = entry.getKey();
            DescriptiveStatistics stats = entry.getValue();
            System.out.println(String.format(formatString, key.getNamespace(), key.getShape(), key.getCommand().name(),
                    stats.getN(), stats.getMin() / NANOS_TO_MILLIS, stats.getMax() / NANOS_TO_MILLIS,
                    stats.getMean() / NANOS_TO_MILLIS, stats.getPercentile(0.95) / NANOS_TO_MILLIS));
        }
    }

    public static void main(String args[]) throws Exception {

        MongoReplay replay = new MongoReplay();
        replay.parseArgs(args);
        replay.execute();
    }

}
