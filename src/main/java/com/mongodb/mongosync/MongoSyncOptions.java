package com.mongodb.mongosync;

import java.util.HashSet;
import java.util.Set;

import com.mongodb.model.Namespace;

public class MongoSyncOptions {
    
    private int threads = 4;
    private int batchSize = 500;
    private String sourceMongoUri;
    private String destMongoUri;
    private boolean dropDestDbs;
    
    private boolean filtered = false;
    private String[] namespaceFilterList;
    
    private Set<Namespace> namespaceFilters = new HashSet<Namespace>();
    private Set<String> databaseFilters = new HashSet<String>();
    
    // populated externally
    private Set<String> namespacesToMigrate;

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public String getSourceMongoUri() {
        return sourceMongoUri;
    }

    public void setSourceMongoUri(String sourceMongoUri) {
        this.sourceMongoUri = sourceMongoUri;
    }

    public String getDestMongoUri() {
        return destMongoUri;
    }

    public void setDestMongoUri(String destMongoUri) {
        this.destMongoUri = destMongoUri;
    }
    
    public void setNamespaceFilters(String[] namespaceFilterList) {
        this.namespaceFilterList = namespaceFilterList;
        if (namespaceFilterList == null) {
            return;
        }
        filtered = true;
        for (String nsStr : namespaceFilterList) {
            if (nsStr.contains("\\.")) {
                Namespace ns = new Namespace(nsStr);
                namespaceFilters.add(ns);
                databaseFilters.add(ns.getDatabaseName());
            } else {
                databaseFilters.add(nsStr);
            }
        }
    }

    public Set<Namespace> getNamespaceFilters() {
        return namespaceFilters;
    }

    public Set<String> getDatabaseFilters() {
        return databaseFilters;
    }

    public void setDropDestDbs(boolean dropDestDbs) {
        this.dropDestDbs = dropDestDbs;
    }

    public boolean isDropDestDbs() {
        return dropDestDbs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Set<String> getNamespacesToMigrate() {
        return namespacesToMigrate;
    }

    public void setNamespacesToMigrate(Set<String> namespacesToMigrate) {
        this.namespacesToMigrate = namespacesToMigrate;
    }
    
    

}
