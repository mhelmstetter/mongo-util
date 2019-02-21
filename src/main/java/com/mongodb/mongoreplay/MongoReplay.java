package com.mongodb.mongoreplay;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.DataFormatException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.Document;

import com.mongodb.util.ShapeUtil;

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

            Set<String> shape = null;
            String shapeStr = null;
            String collName = null;
            Document commandDoc = result.getCommandDoc();
            Document predicates = null;
            if (result.getCommand() == Command.FIND) {
                collName = commandDoc.getString("find");
                result.setCollName(collName);
                predicates = (Document) commandDoc.get("filter");
                if (predicates != null) {
                    shape = ShapeUtil.getShape(predicates);
                }
            } else if (result.getCommand() == Command.UPDATE) {
                collName = commandDoc.getString("update");
                result.setCollName(collName);
                List<Document> updates = (List<Document>)commandDoc.get("updates");
                if (updates != null && updates.size() > 0) {
                    Document first = updates.get(0);
                    System.out.println(commandDoc);
                }
                
            }

            if (shape != null) {
                shapeStr = shape.toString();
            }

            AccumulatorKey key = new AccumulatorKey(result.getDbName(), collName, result.getCommand(), shapeStr);
            DescriptiveStatistics stats = accumulators.get(key);
            if (stats == null) {
                stats = new DescriptiveStatistics();
                accumulators.put(key, stats);
            }
            stats.addValue(result.getDuration());

        }
        logger.debug("DONE processing results");
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
