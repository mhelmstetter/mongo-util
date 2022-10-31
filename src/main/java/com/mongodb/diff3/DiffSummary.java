package com.mongodb.diff3;

import java.util.Date;

public class DiffSummary {
    private final int totalChunks;
    private final long totalDocs;
    private int processedChunks;
    private int processedDocs;
    private int successfulChunks;
    private int successfulDocs;
    private int failedChunks;
    private int failedDocs;
    private final long startTime;

    public DiffSummary(int totalChunks, long totalDocs) {
        this.totalChunks = totalChunks;
        this.totalDocs = totalDocs;
        this.startTime = new Date().getTime();
    }

    public String getSummary() {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);
        int chunksProcessed = getProcessedChunks();
        int docsProcessed = getProcessedDocs();
        int chunksFailed = getFailedChunks();
        int chunksSucceeded = getSuccessfulChunks();
        int docsFailed = getFailedDocs();
        int docsSucceeded = getSuccessfulDocs();

        double chunkProcPct = (chunksProcessed / (double) totalChunks) * 100.;
        double docProcPct = (docsProcessed / (double) totalDocs) * 100.;
        double chunkFailPct = chunksFailed > 0 ? ((double) chunksFailed / (chunksFailed + chunksSucceeded)) * 100. : 0;
        double docFailPct = docsFailed > 0 ? ((double) docsFailed / (docsFailed + docsSucceeded)) * 100. : 0;

        return String.format("%s seconds have elapsed.  " +
                        "%.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%.2f %% of docs processed  (%s/%s docs).  " +
                        "%.2f %% of chunks failed  (%s/%s chunks).  " +
                        "%.2f %% of documents failed  (%s/%s docs).  ", secondsElapsed, chunkProcPct, chunksProcessed,
                totalChunks, docProcPct, docsProcessed, totalDocs, chunkFailPct, chunksFailed, chunksProcessed,
                docFailPct, docsFailed, docsFailed + docsSucceeded);
    }

    public long getTimeElapsed() {
        long now = new Date().getTime();
        return now - startTime;
    }

    public synchronized int getProcessedChunks() {
        return processedChunks;
    }

    public synchronized void incrementProcessedChunks(int num) {
        processedChunks += num;
    }

    public synchronized int getProcessedDocs() {
        return processedDocs;
    }

    public synchronized void incrementProcessedDocs(int num) {
        processedDocs += num;
    }

    public synchronized int getSuccessfulChunks() {
        return successfulChunks;
    }

    public synchronized void incrementSuccessfulChunks(int num) {
        successfulChunks += num;
    }

    public synchronized int getSuccessfulDocs() {
        return successfulDocs;
    }

    public synchronized void incrementSuccessfulDocs(int num) {
        successfulDocs += num;
    }

    public synchronized int getFailedChunks() {
        return failedChunks;
    }

    public synchronized void incrementFailedChunks(int num) {
        failedChunks += num;
    }

    public synchronized int getFailedDocs() {
        return failedDocs;
    }

    public synchronized void incrementFailedDocs(int num) {
        failedDocs += num;
    }
}
