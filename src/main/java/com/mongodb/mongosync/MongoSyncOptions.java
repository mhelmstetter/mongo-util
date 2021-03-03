package com.mongodb.mongosync;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.model.Namespace;

public class MongoSyncOptions {
    
    private int threads = 4;
    private int batchSize = 5000;
    private String sourceMongoUri;
    private String destMongoUri;
    private boolean dropDestDbs;
    private boolean cleanTimestampFiles;
    private boolean useMultiThreadedOplogTailWorkers = false;
    private int oplogThreads = 4;
    private int oplogQueueSize = 50;
    
    
    
    private Set<Namespace> includedNamespaces = new HashSet<Namespace>();
    private Set<String> includedNamespaceStrings = new HashSet<String>();
    private Set<String> includedDatabases = new HashSet<String>();
    private Map<String, Set<String>> includedCollections = new HashMap<>();
    
    private Set<Namespace> excludedNamespaces = new HashSet<Namespace>();
    private Set<String> excludedNamespaceStrings = new HashSet<String>();
    private Set<String> excludedDatabases = new HashSet<String>();

    

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
    
    public boolean includeCollection(String dbName, String collectionName) {
    	boolean filtered = !includedCollections.isEmpty();
    	if (filtered) {
			Set<String> colls = includedCollections.get(dbName);
			if (! colls.contains(collectionName)) {
				return false;
			} else {
				return true;
			}
		}
    	return true;
    }
    
    public boolean includeDatabase(String dbName) {
    	boolean filtered = !includedCollections.isEmpty();
    	if (filtered) {
			return includedCollections.containsKey(dbName);
		}
    	return true;
    }
    
	public void setIncludesExcludes(String[] includes, String[] excludes) {
		
		if (includes != null) {
			Collections.addAll(includedNamespaceStrings, includes);
			for (String nsStr : includes) {
	            if (nsStr.contains(".")) {
	                Namespace ns = new Namespace(nsStr);
	                includedNamespaces.add(ns);
	                includedDatabases.add(ns.getDatabaseName());
	                Set<String> colls = includedCollections.get(ns.getDatabaseName());
					if (colls == null) {
						colls = new HashSet<>();
						includedCollections.put(ns.getDatabaseName(), colls);
					}
					colls.add(ns.getCollectionName());
	            } else {
	                includedDatabases.add(nsStr);
	            }
	        }
		}
		
		
		if (excludes != null) {
			Collections.addAll(excludedNamespaceStrings, excludes);
			for (String nsStr : excludes) {
	            if (nsStr.contains(".")) {
	                Namespace ns = new Namespace(nsStr);
	                excludedNamespaces.add(ns);
	                excludedDatabases.add(ns.getDatabaseName());
	            } else {
	                excludedDatabases.add(nsStr);
	            }
	        }
		}
	}
    
	public boolean excludeDb(String dbName) {
		return excludedDatabases.contains(dbName);
	}
	
	public boolean excludeNamespace(Namespace ns) {
		return excludedNamespaces.contains(ns);
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

	public boolean isCleanTimestampFiles() {
		return cleanTimestampFiles;
	}

	public void setCleanTimestampFiles(boolean cleanTimestampFiles) {
		this.cleanTimestampFiles = cleanTimestampFiles;
	}

	public boolean isUseMultiThreadedOplogTailWorkers() {
		return useMultiThreadedOplogTailWorkers;
	}

	public void setUseMultiThreadedOplogTailWorkers(boolean useMultiThreadedOplogTailWorkers) {
		this.useMultiThreadedOplogTailWorkers = useMultiThreadedOplogTailWorkers;
	}

	public int getOplogThreads() {
		return oplogThreads;
	}

	public void setOplogThreads(int oplogThreads) {
		this.oplogThreads = oplogThreads;
	}

	public int getOplogQueueSize() {
		return oplogQueueSize;
	}

	public void setOplogQueueSize(int oplogQueueSize) {
		this.oplogQueueSize = oplogQueueSize;
	}

	public Set<String> getIncludedNamespaceStrings() {
		return includedNamespaceStrings;
	}

	public Set<Namespace> getIncludedNamespaces() {
		return includedNamespaces;
	}

	public Set<Namespace> getExcludedNamespaces() {
		return excludedNamespaces;
	}

	public Set<String> getExcludedNamespaceStrings() {
		return excludedNamespaceStrings;
	}

	public Map<String, Set<String>> getIncludedCollections() {
		return includedCollections;
	}

}
