package com.mongodb.mongostat;

import java.util.HashMap;
import java.util.Map;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionStats {

    private static Logger logger = LoggerFactory.getLogger(CollectionStats.class);

    private String namespace;
    private String shardName;

    // Per-shard collection stats
    private Long dataSize;
    private Long indexSize;
    private Long totalSize;
    private Long documentCount;

    // WT Cache stats for this collection
    private Long cacheCurrentBytes;
    private Long cacheMaxBytes;
    private Long cacheDirtyBytes;
    private Long cachePageImagesBytes;
    private Long cacheInternalPagesBytes;
    private Long cacheNonPageImagesBytes;
    private Long cachePagesRead;
    private Long cachePagesWritten;
    private Long cacheBytesRead;
    private Long cacheBytesWritten;

    // Server-level max cache for dirty% calculation
    private Long serverMaxCacheBytes;

    // Per-index stats
    private Map<String, IndexStats> indexStats = new HashMap<>();

    // Previous values for delta calculation
    private CollectionStats previous;
    
    public CollectionStats(String namespace, String shardName) {
        this.namespace = namespace;
        this.shardName = shardName;
    }
    
    public void updateFromCollStats(Document collStats) {
        // Store previous values for delta calculation
        previous = new CollectionStats(namespace, shardName);
        copyCurrentToPrevious(previous);

        // Basic collection stats
        dataSize = getLongValue(collStats, "size");
        indexSize = getLongValue(collStats, "totalIndexSize");
        totalSize = getLongValue(collStats, "storageSize");
        documentCount = getLongValue(collStats, "count");

        // WT cache stats from collStats
        Document wiredTiger = (Document) collStats.get("wiredTiger");
        if (wiredTiger != null) {
            Document cache = (Document) wiredTiger.get("cache");
            if (cache != null) {
                cacheCurrentBytes = getLongValue(cache, "bytes currently in the cache");
                cacheMaxBytes = getLongValue(cache, "maximum bytes configured");
                cacheDirtyBytes = getLongValue(cache, "tracked dirty bytes in the cache");
                cachePageImagesBytes = getLongValue(cache, "bytes belonging to page images in the cache");
                cacheInternalPagesBytes = getLongValue(cache, "tracked bytes belonging to internal pages in the cache");
                cacheNonPageImagesBytes = getLongValue(cache, "bytes not belonging to page images in the cache");
                cachePagesRead = getLongValue(cache, "pages read into cache");
                cachePagesWritten = getLongValue(cache, "pages written from cache");
                cacheBytesRead = getLongValue(cache, "bytes read into cache");
                cacheBytesWritten = getLongValue(cache, "bytes written from cache");
            }
        }

        // Index details if provided
        Document indexDetails = (Document) collStats.get("indexDetails");
        Document indexSizes = (Document) collStats.get("indexSizes");
        if (indexDetails != null && indexSizes != null) {
            for (String indexName : indexDetails.keySet()) {
                Document indexDetail = (Document) indexDetails.get(indexName);
                Long indexSizeValue = getLongValue(indexSizes, indexName);

                IndexStats idxStats = indexStats.computeIfAbsent(indexName,
                        k -> new IndexStats(indexName, namespace, shardName));
                idxStats.setServerMaxCacheBytes(serverMaxCacheBytes);
                idxStats.updateFromIndexDetails(indexDetail, indexSizeValue);
            }
        }
    }
    
    private void copyCurrentToPrevious(CollectionStats prev) {
        prev.dataSize = this.dataSize;
        prev.indexSize = this.indexSize;
        prev.totalSize = this.totalSize;
        prev.documentCount = this.documentCount;
        prev.cacheCurrentBytes = this.cacheCurrentBytes;
        prev.cacheMaxBytes = this.cacheMaxBytes;
        prev.cacheDirtyBytes = this.cacheDirtyBytes;
        prev.cachePageImagesBytes = this.cachePageImagesBytes;
        prev.cacheInternalPagesBytes = this.cacheInternalPagesBytes;
        prev.cacheNonPageImagesBytes = this.cacheNonPageImagesBytes;
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

    public double getIndexDirtyFillRatio() {
        // Calculate aggregate index dirty bytes as percentage of server's total max cache
        if (serverMaxCacheBytes == null || serverMaxCacheBytes == 0) {
            return 0.0;
        }
        long totalIndexDirtyBytes = 0L;
        for (IndexStats idx : indexStats.values()) {
            if (idx.getCacheDirtyBytes() != null) {
                totalIndexDirtyBytes += idx.getCacheDirtyBytes();
            }
        }
        return (double) totalIndexDirtyBytes / serverMaxCacheBytes;
    }

    public void setServerMaxCacheBytes(Long serverMaxCacheBytes) {
        this.serverMaxCacheBytes = serverMaxCacheBytes;
    }
    
    // Getters
    public String getNamespace() { return namespace; }
    public String getShardName() { return shardName; }
    public Long getDataSize() { return dataSize; }
    public Long getIndexSize() { return indexSize; }
    public Long getTotalSize() { return totalSize; }
    public Long getDocumentCount() { return documentCount; }
    public Long getCacheCurrentBytes() { return cacheCurrentBytes; }
    public Long getCacheMaxBytes() { return cacheMaxBytes; }
    public Long getCacheDirtyBytes() { return cacheDirtyBytes; }
    public Long getCachePageImagesBytes() { return cachePageImagesBytes; }
    public Long getCacheInternalPagesBytes() { return cacheInternalPagesBytes; }
    public Long getCacheNonPageImagesBytes() { return cacheNonPageImagesBytes; }
    public Long getCachePagesRead() { return cachePagesRead; }
    public Long getCachePagesWritten() { return cachePagesWritten; }
    public Map<String, IndexStats> getIndexStats() { return indexStats; }
    public Long getCacheBytesRead() { return cacheBytesRead; }
    public Long getCacheBytesWritten() { return cacheBytesWritten; }
}