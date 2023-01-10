package com.mongodb.diff3;

import com.mongodb.model.Namespace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

public class DiffSummary {
	private static Logger logger = LoggerFactory.getLogger(DiffSummary.class);

	public enum DiffStatus {
		RUNNING, SUCCEEDED, UNSTARTED, RETRYING, FAILED
	}

	private final LongAdder totalProcessedChunks = new LongAdder();
	private final LongAdder totalProcessedDocs = new LongAdder();
	private final LongAdder totalFailedChunks = new LongAdder();
	private final LongAdder totalFailedDocs = new LongAdder();
	private final LongAdder totalProcessedSize = new LongAdder();
	private final LongAdder totalRetryChunks = new LongAdder();
	private final LongAdder totalSourceOnly = new LongAdder();
	private final LongAdder totalDestOnly = new LongAdder();

	private final Map<Namespace, Map<String, ChunkResult>> chunkResultMap;

	private int totalChunks = -1;
	private final long totalDocs;
	private final long totalSize;
	private final long startTime;
	private final String ppTotalSize;
	private final DiffSummaryClient dbClient;
	private static final long K = 1024;
	private static final long M = 1024 * 1024;
	private static final long G = 1024 * 1024 * 1024;

	public DiffSummary(long totalDocs, long totalSize, DiffSummaryClient dbClient) {
		this.totalDocs = totalDocs;
		this.totalSize = totalSize;
		this.dbClient = dbClient;

		this.ppTotalSize = ppSize(totalSize);
		chunkResultMap = new HashMap<>();
		this.startTime = new Date().getTime();
	}

	public synchronized void setTotalChunks(int totalChunks) {
		this.totalChunks = totalChunks;
	}

	public String getSummary(boolean done) {
		String summary = null;
		try {
			long millsElapsed = getTimeElapsed();
			int secondsElapsed = (int) (millsElapsed / 1000.);

//            DiffSnapshot snapshot = getSnapshot();
//            long totalProcessedChunks = snapshot.getTotalProcessedChunks();
//            long totalProcessedDocs = snapshot.getTotalProcessedDocs();
//            long totalFailedChunks = snapshot.getTotalFailedChunks();
//            long totalFailedDocs = snapshot.getTotalFailedDocs();
//            long totalProcessedSize = snapshot.getTotalProcessedSize();
//            long totalRetryChunks = snapshot.getTotalRetryChunks();
//            long totalSourceOnly = snapshot.getTotalSourceOnly();
//            long totalDestOnly = snapshot.getTotalDestOnly();

			double chunkProcPct = totalChunks >= 0 ? (totalProcessedChunks.longValue() / (double) totalChunks) * 100.
					: 0;
			double docProcPct = (totalProcessedDocs.longValue() / (double) totalDocs) * 100.;
			double sizeProcessedPct = (totalProcessedSize.longValue() / (double) totalSize) * 100.;

			String firstLine = done ? String.format("[Status] Completed in %s seconds.  ", secondsElapsed)
					: String.format("[Status] %s seconds have elapsed.  ", secondsElapsed);
			summary = String.format(
					"%s" + "%.2f %% of chunks processed  (%s/%s chunks).  "
							+ "%.2f %% of docs processed  (%s/%s docs (est.)).  "
							+ "%.2f %% of size processed (%s/%s (est.)).  " + "%d chunks failed.  "
							+ "%d documents mismatched.  " + "%d chunks are retrying.  "
							+ "%s docs found on source only.  %s docs found on target only",
					firstLine, chunkProcPct, totalProcessedChunks.longValue(), totalChunks >= 0 ? totalChunks : "Unknown",
					docProcPct, totalProcessedDocs, totalDocs, sizeProcessedPct, ppSize(totalProcessedSize.longValue()),
					ppTotalSize, totalFailedChunks.longValue(), totalFailedDocs.longValue(), totalRetryChunks.longValue(), 
					totalSourceOnly.longValue(), totalDestOnly.longValue());
		} catch (Exception e) {
			logger.error("getSummary() error", e);
		}

		return summary;
	}

//    private synchronized DiffSnapshot getSnapshot() {
//        long totalProcessedChunks = 0;
//        long totalProcessedDocs = 0;
//        long totalFailedChunks = 0;
//        long totalFailedDocs = 0;
//        long totalProcessedSize = 0;
//        long totalRetryChunks = 0;
//        long totalSourceOnly = 0;
//        long totalDestOnly = 0;
//        for (Map.Entry<Namespace, Map<String, ChunkResult>> nse : chunkResultMap.entrySet()) {
//            for (Map.Entry<String, ChunkResult> e : nse.getValue().entrySet()) {
//                ChunkResult cr = e.getValue();
//                if (cr.getStatus() == DiffStatus.SUCCEEDED || cr.getStatus() == DiffStatus.FAILED) {
//                    totalProcessedChunks++;
//                }
//                long numMismatches = cr.getMismatches().size();
//                long numSourceOnly = cr.getSourceOnly().size();
//                long numDestOnly = cr.getDestOnly().size();
//                totalProcessedDocs += cr.getMatches().longValue() + numMismatches + numSourceOnly + numDestOnly;
//                if ((numMismatches + numSourceOnly + numDestOnly) > 0) {
//                    totalFailedChunks++;
//                }
//                totalFailedDocs += numMismatches;
//                totalProcessedSize += cr.getBytesProcessed().longValue();
//                if (cr.getStatus() == DiffStatus.RETRYING) {
//                    totalRetryChunks++;
//                }
//                totalSourceOnly += numSourceOnly;
//                totalDestOnly += numDestOnly;
//            }
//        }
//
//        return new DiffSnapshot(totalProcessedChunks, totalProcessedDocs, totalFailedChunks, totalFailedDocs,
//                totalProcessedSize, totalRetryChunks, totalSourceOnly, totalDestOnly);
//    }

	private String ppSize(long size) {
		if (size <= 0) {
			return "0 B";
		} else if (size >= G) {
			double convertedSize = (double) size / G;
			if (convertedSize >= 1024) {
				return String.format("%.2f T", convertedSize / 1024.);
			} else {
				return formatSize(size, G, "G");
			}
		} else if (size >= M) {
			return formatSize(size, M, "M");
		} else if (size >= K) {
			return formatSize(size, K, "K");
		} else {
			return String.format("%s B", size);
		}
	}

	private String formatSize(long size, long bound, String indicator) {
		double convertedSize = (double) size / bound;
		return String.format("%.2f %s", convertedSize, indicator);
	}

	public long getTimeElapsed() {
		long now = new Date().getTime();
		return now - startTime;
	}

	private void incrementCountersFromChunkResult(ChunkResult cr) {
		if (cr.getStatus() == DiffStatus.SUCCEEDED || cr.getStatus() == DiffStatus.FAILED) {
			totalProcessedChunks.increment();
		}
		long numMismatches = cr.getMismatches().size();
		long numSourceOnly = cr.getSourceOnly().size();
		long numDestOnly = cr.getDestOnly().size();
		totalProcessedDocs.add(cr.getMatches().longValue() + numMismatches + numSourceOnly + numDestOnly);

		if ((numMismatches + numSourceOnly + numDestOnly) > 0) {
			totalFailedChunks.increment();
		}
		totalFailedDocs.add(numMismatches);
		totalProcessedSize.add(cr.getBytesProcessed().longValue());
		if (cr.getStatus() == DiffStatus.RETRYING) {
			totalRetryChunks.increment();
		}
		totalSourceOnly.add(numSourceOnly);
		totalDestOnly.add(numDestOnly);
	}

	public void updateInitTask(DiffResult result) {
		Namespace ns = result.getNamespace();
		String chunkId = result.getChunkDef().unitString();
		boolean hasFailures = result.getFailedKeys().size() > 0;
		ChunkResult cr = new ChunkResult();
		if (hasFailures) {
			cr.setStatus(DiffStatus.RETRYING);
			cr.getRetryNum().incrementAndGet();
		} else {
			cr.setStatus(DiffStatus.SUCCEEDED);
		}
		cr.addMatches(result.getMatches());
		cr.addBytesProcessed(result.getBytesProcessed());
		cr.setMismatches(result.getMismatchedEntries());
		cr.setSourceOnly(result.getSrcOnlyKeys());
		cr.setDestOnly(result.getDestOnlyKeys());
		
		incrementCountersFromChunkResult(cr);

		if (dbClient != null) {
			dbClient.update(result.getChunkDef(), cr);
		}

		synchronized (chunkResultMap) {
			if (!chunkResultMap.containsKey(ns)) {
				chunkResultMap.put(ns, new HashMap<>());
			}
			Map<String, ChunkResult> nsMap = chunkResultMap.get(ns);
			// Bc this is the initial task, it will always be a new entry
			nsMap.put(chunkId, cr);
		}
	}

	public void updateRetryingDone(DiffResult result) {
		synchronized (chunkResultMap) {
			ChunkResult cr = findChunkResult(result);
			cr.setStatus(DiffStatus.RUNNING);
		}
	}

	public synchronized void updateRetryTask(DiffResult result) {
		int failures = result.getFailedKeys().size();
		ChunkResult cr = null;
		synchronized (chunkResultMap) {
			cr = findChunkResult(result);
			if (failures > 0) {
				if (result.isRetryable()) {
					cr.setStatus(DiffStatus.RETRYING);
					cr.getRetryNum().incrementAndGet();
				} else {
					cr.setStatus(DiffStatus.FAILED);
				}
				cr.setMismatches(result.getMismatchedEntries());
				cr.setDestOnly(result.getDestOnlyKeys());
				cr.setSourceOnly(result.getSrcOnlyKeys());
			} else {
				cr.setStatus(DiffStatus.SUCCEEDED);
				cr.removeMismatches();
				cr.removeSourceOnly();
				cr.removeDestOnly();
			}
			// Currently these won't have been set yet
			cr.addMatches(result.getMatches());
			cr.addBytesProcessed(result.getBytesProcessed());

			if (dbClient != null) {
				dbClient.update(result.getChunkDef(), cr);
			}
		}
		incrementCountersFromChunkResult(cr);
	}

	private ChunkResult findChunkResult(DiffResult result) {
		ChunkResult cr;
		Map<String, ChunkResult> nsMap = chunkResultMap.get(result.getNamespace());
		cr = nsMap.get(result.getChunkDef().unitString());
		if (cr == null) {
			logger.error("Could not find chunk ({}) in summary map", result.getChunkDef().unitString());
			throw new RuntimeException(
					"Could not update chunk: " + result.getChunkDef().unitString() + "; not found in summary map");
		}
		return cr;
	}
}
