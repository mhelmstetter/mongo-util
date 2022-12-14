package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

public class DiffSummary {
    private static Logger logger = LoggerFactory.getLogger(DiffSummary.class);

    public enum DiffStatus {
        RUNNING, SUCCEEDED, UNSTARTED, RETRYING, FAILED
    }

    public static class ChunkResult {
        private final LongAdder matches;
        private final LongAdder bytesProcessed;
        private final Set<BsonValue> mismatches;
        private final Set<BsonValue> sourceOnly;
        private final Set<BsonValue> destOnly;
        private final AtomicInteger retryNum;
        private DiffStatus status;

        ChunkResult() {
            matches = new LongAdder();
            bytesProcessed = new LongAdder();
            mismatches = ConcurrentHashMap.newKeySet();
            sourceOnly = ConcurrentHashMap.newKeySet();
            destOnly = ConcurrentHashMap.newKeySet();
            retryNum = new AtomicInteger(0);
            status = DiffStatus.UNSTARTED;
        }

        public LongAdder getMatches() {
            return matches;
        }

        public void addMatches(long matches) {
            this.matches.add(matches);
        }

        public LongAdder getBytesProcessed() {
            return bytesProcessed;
        }

        public void addBytesProcessed(long bytesProcessed) {
            this.bytesProcessed.add(bytesProcessed);
        }

        public Set<BsonValue> getMismatches() {
            return mismatches;
        }

        public void setMismatches(Set<BsonValue> mismatches) {
            this.mismatches.clear();
            this.mismatches.addAll(mismatches);
        }

        public void removeMismatches() {
            this.mismatches.clear();
        }

        public Set<BsonValue> getSourceOnly() {
            return sourceOnly;
        }

        public void setSourceOnly(Set<BsonValue> sourceOnly) {
            this.sourceOnly.clear();
            this.sourceOnly.addAll(sourceOnly);
        }

        public void removeSourceOnly() {
            this.sourceOnly.clear();
        }

        public Set<BsonValue> getDestOnly() {
            return destOnly;
        }

        public void setDestOnly(Set<BsonValue> destOnly) {
            this.destOnly.clear();
            this.destOnly.addAll(destOnly);
        }

        public void removeDestOnly() {
            this.destOnly.clear();
        }

        public synchronized DiffStatus getStatus() {
            return status;
        }

        public synchronized void setStatus(DiffStatus status) {
            this.status = status;
        }

        public AtomicInteger getRetryNum() {
            return retryNum;
        }
    }

    private static class DiffSnapshot {
        private final long totalProcessedChunks;
        private final long totalProcessedDocs;
        private final long totalFailedChunks;
        private final long totalFailedDocs;
        private final long totalProcessedSize;
        private final long totalRetryChunks;
        private final long totalSourceOnly;
        private final long totalDestOnly;

        DiffSnapshot(long totalProcessedChunks, long totalProcessedDocs, long totalFailedChunks, long totalFailedDocs,
                     long totalProcessedSize, long totalRetryChunks, long totalSourceOnly, long totalDestOnly) {
            this.totalProcessedChunks = totalProcessedChunks;
            this.totalProcessedDocs = totalProcessedDocs;
            this.totalFailedChunks = totalFailedChunks;
            this.totalFailedDocs = totalFailedDocs;
            this.totalProcessedSize = totalProcessedSize;
            this.totalRetryChunks = totalRetryChunks;
            this.totalSourceOnly = totalSourceOnly;
            this.totalDestOnly = totalDestOnly;
        }

        long getTotalProcessedChunks() {
            return totalProcessedChunks;
        }

        long getTotalProcessedDocs() {
            return totalProcessedDocs;
        }

        long getTotalFailedChunks() {
            return totalFailedChunks;
        }

        long getTotalFailedDocs() {
            return totalFailedDocs;
        }

        long getTotalProcessedSize() {
            return totalProcessedSize;
        }

        long getTotalRetryChunks() {
            return totalRetryChunks;
        }

        long getTotalSourceOnly() {
            return totalSourceOnly;
        }

        public long getTotalDestOnly() {
            return totalDestOnly;
        }
    }

    private final Map<Namespace, Map<String, ChunkResult>> m;

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
        m = new HashMap<>();
        this.startTime = new Date().getTime();
    }

    public synchronized void setTotalChunks(int totalChunks) {
        this.totalChunks = totalChunks;
    }

    public String getSummary(boolean done) {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);

        DiffSnapshot snapshot = getSnapshot();
        long totalProcessedChunks = snapshot.getTotalProcessedChunks();
        long totalProcessedDocs = snapshot.getTotalProcessedDocs();
        long totalFailedChunks = snapshot.getTotalFailedChunks();
        long totalFailedDocs = snapshot.getTotalFailedDocs();
        long totalProcessedSize = snapshot.getTotalProcessedSize();
        long totalRetryChunks = snapshot.getTotalRetryChunks();
        long totalSourceOnly = snapshot.getTotalSourceOnly();
        long totalDestOnly = snapshot.getTotalDestOnly();

        double chunkProcPct = totalChunks >= 0 ? (totalProcessedChunks / (double) totalChunks) * 100. : 0;
        double docProcPct = (totalProcessedDocs / (double) totalDocs) * 100.;
        double sizeProcessedPct = (totalProcessedSize / (double) totalSize) * 100.;

        String firstLine = done ? String.format("[Status] Completed in %s seconds.  ", secondsElapsed) :
                String.format("[Status] %s seconds have elapsed.  ", secondsElapsed);
        String summary = String.format("%s" +
                        "%.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%.2f %% of docs processed  (%s/%s docs (est.)).  " +
                        "%.2f %% of size processed (%s/%s (est.)).  " +
                        "%d chunks failed.  " +
                        "%d documents mismatched.  " +
                        "%d chunks are retrying.  " +
                        "%s docs found on source only.  %s docs found on target only", firstLine, chunkProcPct,
                totalProcessedChunks, totalChunks >= 0 ? totalChunks : "Unknown", docProcPct, totalProcessedDocs,
                totalDocs, sizeProcessedPct, ppSize(totalProcessedSize), ppTotalSize, totalFailedChunks,
                totalFailedDocs, totalRetryChunks, totalSourceOnly, totalDestOnly);
        return summary;
    }

    private synchronized DiffSnapshot getSnapshot() {
        long totalProcessedChunks = 0;
        long totalProcessedDocs = 0;
        long totalFailedChunks = 0;
        long totalFailedDocs = 0;
        long totalProcessedSize = 0;
        long totalRetryChunks = 0;
        long totalSourceOnly = 0;
        long totalDestOnly = 0;
        for (Map.Entry<Namespace, Map<String, ChunkResult>> nse : m.entrySet()) {
            for (Map.Entry<String, ChunkResult> e : nse.getValue().entrySet()) {
                ChunkResult cr = e.getValue();
                if (cr.getStatus() == DiffStatus.SUCCEEDED || cr.getStatus() == DiffStatus.FAILED) {
                    totalProcessedChunks++;
                }
                long numMismatches = cr.getMismatches().size();
                long numSourceOnly = cr.getSourceOnly().size();
                long numDestOnly = cr.getDestOnly().size();
                totalProcessedDocs += cr.getMatches().longValue() + numMismatches + numSourceOnly + numDestOnly;
                if ((numMismatches + numSourceOnly + numDestOnly) > 0) {
                    totalFailedChunks++;
                }
                totalFailedDocs += numMismatches;
                totalProcessedSize += cr.getBytesProcessed().longValue();
                if (cr.getStatus() == DiffStatus.RETRYING) {
                    totalRetryChunks++;
                }
                totalSourceOnly += numSourceOnly;
                totalDestOnly += numDestOnly;
            }
        }

        return new DiffSnapshot(totalProcessedChunks, totalProcessedDocs, totalFailedChunks, totalFailedDocs,
                totalProcessedSize, totalRetryChunks, totalSourceOnly, totalDestOnly);
    }

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

    public void updateInitTask(DiffResult result) {
        Namespace ns = result.getNamespace();
        String chunkId = result.getChunkDef().unitString();
        boolean hasFailures = result.getFailedKeys().size() > 0;
        ChunkResult cr = new ChunkResult();
        if (hasFailures) {
            // Currently, we don't update other stats till all the retries are done
            cr.setStatus(DiffStatus.RETRYING);
            cr.getRetryNum().incrementAndGet();
        } else {
            cr.setStatus(DiffStatus.SUCCEEDED);
        }
        cr.addMatches(result.getMatches());
        cr.addBytesProcessed(result.getBytesProcessed());
        cr.setMismatches(result.getMismatchedKeys());
        cr.setSourceOnly(result.getSrcOnlyKeys());
        cr.setDestOnly(result.getDestOnlyKeys());

        if (dbClient != null) {
            dbClient.update(result.getChunkDef(), cr);
        }

        synchronized (m) {
            if (!m.containsKey(ns)) {
                m.put(ns, new HashMap<>());
            }
            Map<String, ChunkResult> nsMap = m.get(ns);
            // Bc this is the initial task, it will always be a new entry
            nsMap.put(chunkId, cr);
        }
    }

    public void updateRetryingDone(DiffResult result) {
        synchronized (m) {
            ChunkResult cr = findChunkResult(result);
            cr.setStatus(DiffStatus.RUNNING);
        }
    }

    public synchronized void updateRetryTask(DiffResult result) {
        int failures = result.getFailedKeys().size();
        synchronized (m) {
            ChunkResult cr = findChunkResult(result);
            if (failures > 0) {
                if (result.isRetryable()) {
                    cr.setStatus(DiffStatus.RETRYING);
                    cr.getRetryNum().incrementAndGet();
                } else {
                    cr.setStatus(DiffStatus.FAILED);
                }
                cr.setMismatches(result.getFailedKeys());
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
    }

    private ChunkResult findChunkResult(DiffResult result) {
        ChunkResult cr;
        Map<String, ChunkResult> nsMap = m.get(result.getNamespace());
        cr = nsMap.get(result.getChunkDef().unitString());
        if (cr == null) {
            logger.error("Could not find chunk ({}) in summary map", result.getChunkDef().unitString());
            throw new RuntimeException("Could not update chunk: " + result.getChunkDef().unitString()
                    + "; not found in summary map");
        }
        return cr;
    }
}
