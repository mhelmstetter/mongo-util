package com.mongodb.mongostat;

import org.bson.Document;

public class ServerStatus {
    
    private String rsName;
    private Document databases;
    
    private long totalInserts = 0;
    private Long currentInserts;
    
    private long totalQueries = 0;
    private Long currentQueries;
    
    private long totalUpdates = 0;
    private Long currentUpdates;
    
    private long totalDeletes = 0;
    private Long currentDeletes;
    
    private Double totalCacheBytesRead = null;
    private Double lastCacheBytesRead = null;

    public void updateServerStatus(Document serverStatus) {
        Document ops = (Document)serverStatus.get("opcounters");
        
        if (rsName == null) {
            Document repl = (Document)serverStatus.get("repl");
            if (repl != null) {
                rsName = repl.getString("setName");
            } else {
                // standalone?
                rsName = serverStatus.getString("host");
            }
            
        }
        long inserts = getLongValue(ops, "insert");
        if (currentInserts == null) {
            currentInserts = 0L;
        } else {
            currentInserts = inserts - totalInserts;
        }
        totalInserts = inserts;
        
        long updates = getLongValue(ops, "update");
        if (currentUpdates == null) {
            currentUpdates = 0L;
        } else {
            currentUpdates = updates - totalUpdates;
        }
        totalUpdates = updates;
        
        long queries = getLongValue(ops, "query");
        if (currentQueries == null) {
            currentQueries = 0L;
        } else {
            currentQueries = queries - totalQueries;
        }
        totalQueries = queries;
        
        long deletes = getLongValue(ops, "delete");
        if (currentDeletes == null) {
            currentDeletes = 0L;
        } else {
            currentDeletes = deletes - totalDeletes;
        }
        totalDeletes = deletes;
        
        Document wiredTiger = (Document)serverStatus.get("wiredTiger");
        if (wiredTiger != null) {
            Document cache = (Document)wiredTiger.get("cache");
            Number cacheBytesNum = (Number)cache.get("bytes read into cache");
            Double cacheBytes = cacheBytesNum.doubleValue();
            if (lastCacheBytesRead != null) {
                Double current = cacheBytes - lastCacheBytesRead;
                totalCacheBytesRead += current;
            } else {
                totalCacheBytesRead = 0.0;
            }
            lastCacheBytesRead = cacheBytes;
            
        }
    }
    
    public void report() {
        // "%-25s%-30s%-25s%-25s%-25s%n",
        System.out.printf(
                "%-15s%8s%8s%8s%8s %13s%13s%13s%13s %,8.0f %n",
                rsName, currentInserts, currentQueries, currentUpdates, currentDeletes,
                totalInserts, totalQueries, totalUpdates, totalDeletes, totalCacheBytesRead/1024/1024);
    }
    
    private long getLongValue(Document doc, String key) {
        Number num = (Number) doc.get(key);
        return num != null ? num.longValue() : 0L;
    }
    
    // Getters for JSON output
    public String getRsName() { return rsName; }
    public Long getCurrentInserts() { return currentInserts; }
    public Long getCurrentQueries() { return currentQueries; }
    public Long getCurrentUpdates() { return currentUpdates; }
    public Long getCurrentDeletes() { return currentDeletes; }
    public Long getTotalInserts() { return totalInserts; }
    public Long getTotalQueries() { return totalQueries; }
    public Long getTotalUpdates() { return totalUpdates; }
    public Long getTotalDeletes() { return totalDeletes; }
    public Double getTotalCacheBytesRead() { return totalCacheBytesRead; }

}
