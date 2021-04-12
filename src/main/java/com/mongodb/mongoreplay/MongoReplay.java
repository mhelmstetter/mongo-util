package com.mongodb.mongoreplay;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.zip.DataFormatException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class MongoReplay extends AbstractMongoReplayUtil {

	private Map<AccumulatorKey, DescriptiveStatistics> accumulators = new HashMap<AccumulatorKey, DescriptiveStatistics>();

	private int NANOS_TO_MILLIS = 1000000;

	public void executeSplitMode() throws FileNotFoundException, DataFormatException, NoSuchMethodException,
			SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		init();
		for (String filename : fileNames) {
			this.replayFileSplitMode(filename);
		}
		processResults();
	}

	public void execute() throws NoSuchMethodException, SecurityException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, FileNotFoundException, DataFormatException {
		init();
		for (String filename : fileNames) {
			replayFile(filename);
		}
		processResults();
	}
	
	private void accumulateResult(ReplayResult result) {
		if (result == null || result.getCollectionName() == null) {
			System.out.println("xxx");
		}
		
		AccumulatorKey key = new AccumulatorKey(result.getDbName(), result.getCollectionName(),
				result.getCommand(), result.getQueryShape());
		DescriptiveStatistics stats = accumulators.get(key);
		if (stats == null) {
			stats = new DescriptiveStatistics();
			accumulators.put(key, stats);
		}
		stats.addValue(result.getDuration());
	}

	@SuppressWarnings("unchecked")
	private void processResults() {
		logger.debug("Processing results");
		for (Future<?> future : futures) {
			Object res = null;
			try {
				res = future.get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Error getting future", e);
			}

			if (res == null) {
				continue;
			}

			if (res instanceof ReplayResult) {
				ReplayResult result = (ReplayResult) res;
				
			} else if (res instanceof String) {
				logger.debug("String result: " + res);
			} else if (res instanceof List) {
				List<ReplayResult> results = (List<ReplayResult>)res;
				for (ReplayResult result : results) {
					accumulateResult(result);
				}
				
			} else {
				logger.debug("Unk result: " + res);
			}

		}
		logger.debug("DONE processing results");
		if (pool.isPaused()) {
			logger.debug("executor is paused, resuming");
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

		String formatString = "%-" + maxNamespaceLen + "s %-" + maxShapeLen
				+ "s %-16s %-10d %-10.0f %-10.0f %-10.0f %-10.0f";
		String headerFormatString = "%-" + maxNamespaceLen + "s %-" + maxShapeLen
				+ "s %-16s %-10s %-10s %-10s %-10s %-10s";
		System.out.println(String.format(headerFormatString, "namespace", "query shape", "cmd", "count", "min", "max",
				"avg", "95p"));

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
		if (replay.isSplitFilesMode()) {
			replay.executeSplitMode();
		} else {
			replay.execute();
		}

	}

}
