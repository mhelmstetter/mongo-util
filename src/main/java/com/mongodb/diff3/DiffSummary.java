package com.mongodb.diff3;

import java.util.Date;

public class DiffSummary {
    private final int totalChunks;
    private final long totalDocs;
    private int processedChunks;
    private long processedDocs;
    private int successfulChunks;
    private long successfulDocs;
    private int failedChunks;
    private long failedDocs;
    private final long startTime;

    public DiffSummary(int totalChunks, long totalDocs) {
        this.totalChunks = totalChunks;
        this.totalDocs = totalDocs;
        this.startTime = new Date().getTime();
    }

    public String getSummary() {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);

        double chunkProcPct = (processedChunks / (double) totalChunks) * 100.;
        double docProcPct = (processedDocs / (double) totalDocs) * 100.;
        double chunkFailPct = failedChunks > 0 ? ((double) failedChunks / (failedChunks + successfulChunks)) * 100. : 0;
        double docFailPct = failedDocs > 0 ? ((double) failedDocs / (failedDocs + successfulDocs)) * 100. : 0;

        return String.format("%s seconds have elapsed.  " +
                        "%.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%.2f %% of docs processed  (%s/%s docs).  " +
                        "%.2f %% of chunks failed  (%s/%s chunks).  " +
                        "%.2f %% of documents failed  (%s/%s docs).  ", secondsElapsed, chunkProcPct, processedChunks,
                totalChunks, docProcPct, processedDocs, totalDocs, chunkFailPct, failedChunks, processedChunks,
                docFailPct, failedDocs, processedDocs);
    }

    public long getTimeElapsed() {
        long now = new Date().getTime();
        return now - startTime;
    }

    public void incrementProcessedChunks(int num) {
        processedChunks += num;
    }

    public void incrementProcessedDocs(long num) {
        processedDocs += num;
    }

    public void incrementSuccessfulChunks(int num) {
        successfulChunks += num;
    }

    public void incrementSuccessfulDocs(long num) {
        successfulDocs += num;
    }

    public void incrementFailedChunks(int num) {
        failedChunks += num;
    }

    public void incrementFailedDocs(int num) {
        failedDocs += num;
    }
}
