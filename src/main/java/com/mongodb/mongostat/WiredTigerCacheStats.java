package com.mongodb.mongostat;

import org.bson.Document;

public class WiredTigerCacheStats {
    
    private String shardName;
    
    // Memory usage
    private Long currentCacheBytes;
    private Long maxCacheBytes;
    private Long dirtyBytes;
    private Long internalPagesBytes;
    private Long pageImagesBytes;
    private Long nonPageImagesBytes;
    
    // Page activity  
    private Long pagesReadIntoCache;
    private Long pagesWrittenFromCache;
    private Long pagesEvictedByAppThreads;
    private Long pagesEvictedByWorkerThreads;
    
    // Eviction behavior
    private Long pagesSelectedUnableToEvict;
    private Long pagesEvictedExceededMax;
    private Long pagesEvictedDeletedChains;
    
    // Dirty vs clean
    private Long dirtyPagesCount;
    private Long modifiedPagesEvicted;
    private Long unmodifiedPagesEvicted;
    
    // Concurrency pressure
    private Long appThreadsPageReads;
    private Long appThreadsPageWrites;
    private Long appThreadsPageEvicted;
    
    // Previous values for delta calculation
    private WiredTigerCacheStats previous;
    
    public WiredTigerCacheStats(String shardName) {
        this.shardName = shardName;
    }
    
    public void updateFromServerStatus(Document serverStatus) {
        Document wiredTiger = (Document) serverStatus.get("wiredTiger");
        if (wiredTiger == null) {
            return;
        }
        
        Document cache = (Document) wiredTiger.get("cache");
        if (cache == null) {
            return;
        }
        
        // Store previous values for delta calculation
        previous = new WiredTigerCacheStats(shardName);
        copyCurrentToPrevious(previous);
        
        // Memory usage
        currentCacheBytes = getLongValue(cache, "bytes currently in the cache");
        maxCacheBytes = getLongValue(cache, "maximum bytes configured");
        dirtyBytes = getLongValue(cache, "tracked dirty bytes in the cache");
        internalPagesBytes = getLongValue(cache, "tracked bytes belonging to internal pages");
        pageImagesBytes = getLongValue(cache, "bytes belonging to page images in the cache");
        nonPageImagesBytes = getLongValue(cache, "bytes not belonging to page images in the cache");
        
        // Page activity
        pagesReadIntoCache = getLongValue(cache, "pages read into cache");
        pagesWrittenFromCache = getLongValue(cache, "pages written from cache");
        pagesEvictedByAppThreads = getLongValue(cache, "application threads page evicted due to exceeding the in-memory maximum count");
        pagesEvictedByWorkerThreads = getLongValue(cache, "pages evicted by eviction worker threads");
        
        // Eviction behavior
        pagesSelectedUnableToEvict = getLongValue(cache, "pages selected for eviction unable to be evicted");
        pagesEvictedExceededMax = getLongValue(cache, "pages evicted because they exceeded the in-memory maximum");
        pagesEvictedDeletedChains = getLongValue(cache, "pages evicted because they had chains of deleted items");
        
        // Dirty vs clean
        dirtyPagesCount = getLongValue(cache, "tracked dirty pages in the cache");
        modifiedPagesEvicted = getLongValue(cache, "modified pages evicted");
        unmodifiedPagesEvicted = getLongValue(cache, "unmodified pages evicted");
        
        // Concurrency pressure
        appThreadsPageReads = getLongValue(cache, "application threads page read from disk to cache count");
        appThreadsPageWrites = getLongValue(cache, "application threads page write from cache to disk count");
        appThreadsPageEvicted = getLongValue(cache, "application threads page evicted due to exceeding the in-memory maximum count");
    }
    
    private void copyCurrentToPrevious(WiredTigerCacheStats prev) {
        prev.currentCacheBytes = this.currentCacheBytes;
        prev.maxCacheBytes = this.maxCacheBytes;
        prev.dirtyBytes = this.dirtyBytes;
        prev.internalPagesBytes = this.internalPagesBytes;
        prev.pageImagesBytes = this.pageImagesBytes;
        prev.nonPageImagesBytes = this.nonPageImagesBytes;
        prev.pagesReadIntoCache = this.pagesReadIntoCache;
        prev.pagesWrittenFromCache = this.pagesWrittenFromCache;
        prev.pagesEvictedByAppThreads = this.pagesEvictedByAppThreads;
        prev.pagesEvictedByWorkerThreads = this.pagesEvictedByWorkerThreads;
        prev.pagesSelectedUnableToEvict = this.pagesSelectedUnableToEvict;
        prev.pagesEvictedExceededMax = this.pagesEvictedExceededMax;
        prev.pagesEvictedDeletedChains = this.pagesEvictedDeletedChains;
        prev.dirtyPagesCount = this.dirtyPagesCount;
        prev.modifiedPagesEvicted = this.modifiedPagesEvicted;
        prev.unmodifiedPagesEvicted = this.unmodifiedPagesEvicted;
        prev.appThreadsPageReads = this.appThreadsPageReads;
        prev.appThreadsPageWrites = this.appThreadsPageWrites;
        prev.appThreadsPageEvicted = this.appThreadsPageEvicted;
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
            case "pagesReadIntoCache": return pagesReadIntoCache;
            case "pagesWrittenFromCache": return pagesWrittenFromCache;
            case "pagesEvictedByAppThreads": return pagesEvictedByAppThreads;
            case "pagesEvictedByWorkerThreads": return pagesEvictedByWorkerThreads;
            case "pagesSelectedUnableToEvict": return pagesSelectedUnableToEvict;
            case "pagesEvictedExceededMax": return pagesEvictedExceededMax;
            case "pagesEvictedDeletedChains": return pagesEvictedDeletedChains;
            case "modifiedPagesEvicted": return modifiedPagesEvicted;
            case "unmodifiedPagesEvicted": return unmodifiedPagesEvicted;
            case "appThreadsPageReads": return appThreadsPageReads;
            case "appThreadsPageWrites": return appThreadsPageWrites;
            case "appThreadsPageEvicted": return appThreadsPageEvicted;
            default: return null;
        }
    }
    
    public double getDirtyFillRatio() {
        if (currentCacheBytes == null || currentCacheBytes == 0 || dirtyBytes == null) {
            return 0.0;
        }
        return (double) dirtyBytes / currentCacheBytes;
    }
    
    public double getCacheUtilization() {
        if (maxCacheBytes == null || maxCacheBytes == 0 || currentCacheBytes == null) {
            return 0.0;
        }
        return (double) currentCacheBytes / maxCacheBytes;
    }
    
    // Getters
    public String getShardName() { return shardName; }
    public Long getCurrentCacheBytes() { return currentCacheBytes; }
    public Long getMaxCacheBytes() { return maxCacheBytes; }
    public Long getDirtyBytes() { return dirtyBytes; }
    public Long getInternalPagesBytes() { return internalPagesBytes; }
    public Long getPageImagesBytes() { return pageImagesBytes; }
    public Long getNonPageImagesBytes() { return nonPageImagesBytes; }
    public Long getPagesReadIntoCache() { return pagesReadIntoCache; }
    public Long getPagesWrittenFromCache() { return pagesWrittenFromCache; }
    public Long getPagesEvictedByAppThreads() { return pagesEvictedByAppThreads; }
    public Long getPagesEvictedByWorkerThreads() { return pagesEvictedByWorkerThreads; }
    public Long getPagesSelectedUnableToEvict() { return pagesSelectedUnableToEvict; }
    public Long getPagesEvictedExceededMax() { return pagesEvictedExceededMax; }
    public Long getPagesEvictedDeletedChains() { return pagesEvictedDeletedChains; }
    public Long getDirtyPagesCount() { return dirtyPagesCount; }
    public Long getModifiedPagesEvicted() { return modifiedPagesEvicted; }
    public Long getUnmodifiedPagesEvicted() { return unmodifiedPagesEvicted; }
    public Long getAppThreadsPageReads() { return appThreadsPageReads; }
    public Long getAppThreadsPageWrites() { return appThreadsPageWrites; }
    public Long getAppThreadsPageEvicted() { return appThreadsPageEvicted; }
}