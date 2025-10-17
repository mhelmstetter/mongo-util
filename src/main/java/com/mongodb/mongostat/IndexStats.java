package com.mongodb.mongostat;

import org.bson.Document;

public class IndexStats {

    private String indexName;
    private String namespace;
    private String shardName;

    // Index size
    private Long indexSize;

    // WT Cache stats for this index
    private Long cacheCurrentBytes;
    private Long cacheDirtyBytes;
    private Long cachePagesRead;
    private Long cachePagesWritten;
    private Long cacheBytesRead;
    private Long cacheBytesWritten;

    // Server-level max cache for dirty% calculation
    private Long serverMaxCacheBytes;

    // Previous values for delta calculation
    private IndexStats previous;

    public IndexStats(String indexName, String namespace, String shardName) {
        this.indexName = indexName;
        this.namespace = namespace;
        this.shardName = shardName;
    }

    public void updateFromIndexDetails(Document indexDetails, Long indexSize) {
        // Store previous values for delta calculation
        previous = new IndexStats(indexName, namespace, shardName);
        copyCurrentToPrevious(previous);

        this.indexSize = indexSize;

        // WT cache stats from index details
        if (indexDetails != null) {
            Document cache = (Document) indexDetails.get("cache");
            if (cache != null) {
                cacheCurrentBytes = getLongValue(cache, "bytes currently in the cache");
                cacheDirtyBytes = getLongValue(cache, "tracked dirty bytes in the cache");
                cachePagesRead = getLongValue(cache, "pages read into cache");
                cachePagesWritten = getLongValue(cache, "pages written from cache");
                cacheBytesRead = getLongValue(cache, "bytes read into cache");
                cacheBytesWritten = getLongValue(cache, "bytes written from cache");
            }
        }
    }

    private void copyCurrentToPrevious(IndexStats prev) {
        prev.indexSize = this.indexSize;
        prev.cacheCurrentBytes = this.cacheCurrentBytes;
        prev.cacheDirtyBytes = this.cacheDirtyBytes;
        prev.cachePagesRead = this.cachePagesRead;
        prev.cachePagesWritten = this.cachePagesWritten;
        prev.cacheBytesRead = this.cacheBytesRead;
        prev.cacheBytesWritten = this.cacheBytesWritten;
    }

    private Long getLongValue(Document doc, String key) {
        Number num = (Number) doc.get(key);
        return num != null ? num.longValue() : 0L;
    }

    public Long getDelta(String statName) {
        if (previous == null) {
            return 0L;
        }

        Long current = getStatValue(statName);
        Long prev = previous.getStatValue(statName);

        if (current == null || prev == null) {
            return 0L;
        }

        return current - prev;
    }

    private Long getStatValue(String statName) {
        switch (statName) {
            case "cachePagesRead": return cachePagesRead;
            case "cachePagesWritten": return cachePagesWritten;
            case "cacheBytesRead": return cacheBytesRead;
            case "cacheBytesWritten": return cacheBytesWritten;
            default: return null;
        }
    }

    public double getDirtyFillRatio() {
        // Calculate dirty bytes as percentage of server's total max cache
        if (serverMaxCacheBytes == null || serverMaxCacheBytes == 0 || cacheDirtyBytes == null) {
            return 0.0;
        }
        return (double) cacheDirtyBytes / serverMaxCacheBytes;
    }

    public void setServerMaxCacheBytes(Long serverMaxCacheBytes) {
        this.serverMaxCacheBytes = serverMaxCacheBytes;
    }

    // Getters
    public String getIndexName() { return indexName; }
    public String getNamespace() { return namespace; }
    public String getShardName() { return shardName; }
    public Long getIndexSize() { return indexSize; }
    public Long getCacheCurrentBytes() { return cacheCurrentBytes; }
    public Long getCacheDirtyBytes() { return cacheDirtyBytes; }
    public Long getCachePagesRead() { return cachePagesRead; }
    public Long getCachePagesWritten() { return cachePagesWritten; }
    public Long getCacheBytesRead() { return cacheBytesRead; }
    public Long getCacheBytesWritten() { return cacheBytesWritten; }
}
