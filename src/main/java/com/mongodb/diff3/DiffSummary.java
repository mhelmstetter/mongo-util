package com.mongodb.diff3;

import java.util.Date;

public class DiffSummary {
    private final int totalChunks;
    private final long totalDocs;
    private final long totalSize;
    private int processedChunks;
    private long processedDocs;
    private long processedSize;
    private int successfulChunks;
    private long successfulDocs;
    private int failedChunks;
    private long failedDocs;
    private final long startTime;
    private long sourceOnly;
    private long destOnly;
    private String ppTotalSize;
    private static final long K = 1024;
    private static final long M = 1024 * 1024;
    private static final long G = 1024 * 1024 * 1024;

    public DiffSummary(int totalChunks, long totalDocs, long totalSize) {
        this.totalChunks = totalChunks;
        this.totalDocs = totalDocs;
        this.totalSize = totalSize;
        this.ppTotalSize = ppSize(this.totalSize);
        this.startTime = new Date().getTime();
    }

    public String getSummary(boolean done) {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);

        double chunkProcPct = (processedChunks / (double) totalChunks) * 100.;
        double docProcPct = (processedDocs / (double) totalDocs) * 100.;
        double chunkFailPct = failedChunks > 0 ? ((double) failedChunks / (failedChunks + successfulChunks)) * 100. : 0;
        double docFailPct = failedDocs > 0 ? ((double) failedDocs / (failedDocs + successfulDocs)) * 100. : 0;
        double sizeProcessedPct = (processedSize / (double) totalSize) * 100.;

        String firstLine = done ? String.format("Completed in %s seconds.  ", secondsElapsed) :
                String.format("%s seconds have elapsed.  ", secondsElapsed);
        return String.format("%s" +
                        "%6.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%6.2f %% of docs processed  (%s/%s docs).  " +
                        "%6.2f %% of size processed (%s/%s).  " +
                        "%6.2f %% of chunks failed  (%s/%s chunks).  " +
                        "%6.2f %% of documents failed  (%s/%s docs).  " +
                        "%s docs found on source only.  %s docs found on target only", firstLine, chunkProcPct,
                processedChunks, totalChunks, docProcPct, processedDocs, totalDocs, sizeProcessedPct,
                ppSize(processedSize), ppTotalSize, chunkFailPct, failedChunks, processedChunks, docFailPct,
                failedDocs, processedDocs, sourceOnly, destOnly);
    }

    private String ppSize(long size) {
        if (size <= 0){
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

    public void incrementSourceOnly(long num) {
        sourceOnly += num;
    }

    public void incrementDestOnly(long num) {
        destOnly += num;
    }

    public void incrementProcessedSize(long num) {
        processedSize += num;
    }
}
