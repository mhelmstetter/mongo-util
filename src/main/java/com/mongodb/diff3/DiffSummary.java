package com.mongodb.diff3;

import java.util.Date;

public class DiffSummary {
    private int totalChunks = -1;
    private final long totalDocs;
    private final long totalSize;
    private long processedChunks;
    private long processedDocs;
    private long processedSize;
    private long successfulDocs;
    private long failedChunks;
    private long failedDocs;
    private long retryChunks;
    private final long startTime;
    private long sourceOnly;
    private long destOnly;
    private final String ppTotalSize;
    private static final long K = 1024;
    private static final long M = 1024 * 1024;
    private static final long G = 1024 * 1024 * 1024;

    public DiffSummary(long totalDocs, long totalSize) {
        this.totalDocs = totalDocs;
        this.totalSize = totalSize;

        this.ppTotalSize = ppSize(totalSize);
        this.startTime = new Date().getTime();
    }

    public synchronized void setTotalChunks(int totalChunks) {
        this.totalChunks = totalChunks;
    }

    public String getSummary(boolean done) {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);

        double chunkProcPct = totalChunks >= 0 ? (processedChunks / (double) totalChunks) * 100. : 0;
        double docProcPct = (processedDocs / (double) totalDocs) * 100.;
        double chunkFailPct = failedChunks > 0 ? ((double) failedChunks /
                processedChunks) * 100. : 0;
        double docFailPct = failedDocs > 0 ? ((double) failedDocs) /
                (failedDocs + successfulDocs) * 100. : 0;
        double sizeProcessedPct = (processedSize / (double) totalSize) * 100.;

        String firstLine = done ? String.format("[Status] Completed in %s seconds.  ", secondsElapsed) :
                String.format("[Status] %s seconds have elapsed.  ", secondsElapsed);
        return String.format("%s" +
                        "%.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%.2f %% of docs processed  (%s/%s docs).  " +
                        "%.2f %% of size processed (%s/%s).  " +
                        "%.2f %% of chunks failed  (%s/%s chunks).  " +
                        "%.2f %% of documents failed  (%s/%s docs).  " +
                        "%d chunks are retrying.  " +
                        "%s docs found on source only.  %s docs found on target only", firstLine, chunkProcPct,
                processedChunks, totalChunks >= 0 ? totalChunks : "Unknown", docProcPct, processedDocs, totalDocs,
                sizeProcessedPct, ppSize(processedSize), ppTotalSize, chunkFailPct,
                failedChunks, processedChunks, docFailPct, failedDocs,
                processedDocs, retryChunks, sourceOnly, destOnly);
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

    public synchronized void updateInitTask(DiffResult result) {
        int failures = result.getFailureCount();
        if (failures > 0){
            retryChunks++;
        } else {
            successfulDocs += result.getMatches();
            processedDocs += result.getMatches();
            processedChunks++;
            processedSize += result.getBytesProcessed();
        }
    }

    public synchronized void updateRetryingDone(DiffResult result){
        retryChunks--;
    }

    public synchronized void updateRetryTask(DiffResult result) {
        int failures = result.getFailureCount();
        if (failures > 0) {
            failedChunks++;
        }
        successfulDocs += result.getMatches() - failures;
//        retryChunks--;
        processedDocs += result.getMatches() + failures;
        failedDocs += failures;
        processedChunks++;
        sourceOnly += result.getOnlyOnSourceCount();
        destOnly += result.getOnlyOnDestCount();
        processedSize += result.getBytesProcessed();
    }
}
