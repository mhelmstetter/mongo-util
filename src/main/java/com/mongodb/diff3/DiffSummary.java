package com.mongodb.diff3;

import java.util.Date;
import java.util.concurrent.atomic.LongAdder;

public class DiffSummary {
    private final int totalChunks;
    private final long totalDocs;
    private final long totalSize;
    private LongAdder processedChunks;
    private LongAdder processedDocs;
    private LongAdder processedSize;
    private LongAdder successfulDocs;
    private LongAdder failedChunks;
    private LongAdder failedDocs;
    private LongAdder retryChunks;
    private final long startTime;
    private LongAdder sourceOnly;
    private LongAdder destOnly;
    private String ppTotalSize;
    private static final long K = 1024;
    private static final long M = 1024 * 1024;
    private static final long G = 1024 * 1024 * 1024;

    public DiffSummary(int totalChunks, long totalDocs, long totalSize) {
        this.totalChunks = totalChunks;
        this.totalDocs = totalDocs;
        this.totalSize = totalSize;

        processedDocs = new LongAdder();
        processedChunks = new LongAdder();
        processedSize = new LongAdder();
        successfulDocs = new LongAdder();
        failedChunks = new LongAdder();
        failedDocs = new LongAdder();
        sourceOnly = new LongAdder();
        destOnly = new LongAdder();
        retryChunks = new LongAdder();

        this.ppTotalSize = ppSize(totalSize);
        this.startTime = new Date().getTime();
    }

    public String getSummary(boolean done) {
        long millsElapsed = getTimeElapsed();
        int secondsElapsed = (int) (millsElapsed / 1000.);

        double chunkProcPct = (processedChunks.longValue() / (double) totalChunks) * 100.;
        double docProcPct = (processedDocs.longValue() / (double) totalDocs) * 100.;
        double chunkFailPct = failedChunks.longValue() > 0 ? ((double) failedChunks.longValue() /
                processedChunks.longValue()) * 100. : 0;
        double docFailPct = failedDocs.longValue() > 0 ? ((double) failedDocs.longValue() /
                (failedDocs.longValue() + successfulDocs.longValue())) * 100. : 0;
        double sizeProcessedPct = (processedSize.longValue() / (double) totalSize) * 100.;

        String firstLine = done ? String.format("Completed in %s seconds.  ", secondsElapsed) :
                String.format("%s seconds have elapsed.  ", secondsElapsed);
        return String.format("%s" +
                        "%.2f %% of chunks processed  (%s/%s chunks).  " +
                        "%.2f %% of docs processed  (%s/%s docs).  " +
                        "%.2f %% of size processed (%s/%s).  " +
                        "%.2f %% of chunks failed  (%s/%s chunks).  " +
                        "%.2f %% of documents failed  (%s/%s docs).  " +
                        "%d chunks are retrying.  " +
                        "%s docs found on source only.  %s docs found on target only", firstLine, chunkProcPct,
                processedChunks.longValue(), totalChunks, docProcPct, processedDocs.longValue(), totalDocs,
                sizeProcessedPct, ppSize(processedSize.longValue()), ppTotalSize, chunkFailPct,
                failedChunks.longValue(), processedChunks.longValue(), docFailPct, failedDocs.longValue(),
                processedDocs.longValue(), retryChunks.intValue(), sourceOnly.longValue(), destOnly.longValue());
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

    public void incrementProcessedChunks(int num) {
        processedChunks.add(num);
    }

    public void incrementProcessedDocs(long num) {
        processedDocs.add(num);
    }

    public void incrementSuccessfulDocs(long num) {
        successfulDocs.add(num);
    }

    public void incrementFailedChunks(int num) {
        failedChunks.add(num);
    }

    public void incrementFailedDocs(int num) {
        failedDocs.add(num);
    }

    public void incrementSourceOnly(long num) {
        sourceOnly.add(num);
    }

    public void incrementDestOnly(long num) {
        destOnly.add(num);
    }

    public void incrementProcessedSize(long num) {
        processedSize.add(num);
    }

    public void incrementRetryChunks(long num) {
        retryChunks.add(num);
    }
}
