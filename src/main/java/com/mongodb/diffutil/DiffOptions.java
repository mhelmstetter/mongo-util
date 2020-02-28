package com.mongodb.diffutil;

import java.util.HashSet;
import java.util.Set;

import com.mongodb.model.Namespace;

public class DiffOptions {
    
    private int threads = 4;
    private int queueSize = 250000;
    private String sourceMongoUri;
    private String destMongoUri;
    private String[] shardMap;


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

	public int getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(int queueSize) {
		this.queueSize = queueSize;
	}

	public String[] getShardMap() {
		return shardMap;
	}

	public void setShardMap(String[] shardMap) {
		this.shardMap = shardMap;
	}
    



    
    

}
