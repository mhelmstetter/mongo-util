package com.mongodb.mongostat;

import java.util.List;

import org.bson.Document;

public class ServerStatus {
    
    private String rsName;
    private Document databases;
    
    private int totalInserts = 0;
    private Integer currentInserts;
    
    private int totalQueries = 0;
    private Integer currentQueries;
    
    private int totalUpdates = 0;
    private Integer currentUpdates;
    
    private int totalDeletes = 0;
    private Integer currentDeletes;
    
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
        int inserts = ops.getInteger("insert");
        if (currentInserts == null) {
            currentInserts = 0;
        } else {
            currentInserts = inserts - totalInserts;
        }
        totalInserts = inserts;
        
        int updates = ops.getInteger("update");
        if (currentUpdates == null) {
            currentUpdates = 0;
        } else {
            currentUpdates = updates - totalUpdates;
        }
        totalUpdates = updates;
        
        int queries = ops.getInteger("query");
        if (currentQueries == null) {
            currentQueries = 0;
        } else {
            currentQueries = queries - totalQueries;
        }
        totalQueries = queries;
        
        int deletes = ops.getInteger("delete");
        if (currentDeletes == null) {
            currentDeletes = 0;
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

    public void setDatabases(Document databases) {
        this.databases = databases;
        List<Document> dbList = (List<Document>)databases.get("databases");
        double total = 0;
        for (Document dbDoc : dbList) {
            Double size = (Double)dbDoc.get("sizeOnDisk");
            total += size;
        }
        System.out.printf("%,.2f %n", total/1024/1024);
        
    }

}
