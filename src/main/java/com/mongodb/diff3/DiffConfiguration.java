package com.mongodb.diff3;

import static com.mongodb.client.model.Filters.regex;

import java.util.*;

import org.bson.Document;

import com.mongodb.model.Namespace;

public class DiffConfiguration {
	public final static String PARTITION_MODE = "partition";
	public final static String SHARD_MODE = "shard";
	public String sourceClusterUri;
	public String destClusterUri;
	private boolean filtered;
	private final Document chunkQuery = new Document();
	private int threads = 8;
	private double sampleRate;
	private int sampleMinDocs;
	private int maxDocsToSamplePerPartition;
	private long defaultPartitionSize;
	private String mode;
	private int maxRetries;
	private boolean useStatusDb;
	private String statusDbUri;
	private String statusDbName;
	private String statusDbCollName;
	private final String[] knownModes = new String[]{PARTITION_MODE, SHARD_MODE};

	private Set<Namespace> includeNamespaces = new HashSet<>();
	private Set<String> includeDatabases = new HashSet<>();

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		if (!(Arrays.binarySearch(knownModes, mode) >= 0)){
			throw new RuntimeException("Unknown mode: " + mode);
		}
		this.mode = mode;
	}

	public String getSourceClusterUri() {
		return sourceClusterUri;
	}

	public void setSourceClusterUri(String sourceClusterUri) {
		this.sourceClusterUri = sourceClusterUri;
	}

	public String getDestClusterUri() {
		return destClusterUri;
	}

	public void setDestClusterUri(String destClusterUri) {
		this.destClusterUri = destClusterUri;
	}

	public Set<Namespace> getIncludeNamespaces() {
		return includeNamespaces;
	}

	public void setIncludeNamespaces(Set<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}

	public Set<String> getIncludeDatabases() {
		return includeDatabases;
	}

	public void setIncludeDatabases(Set<String> includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null) {
			initializeChunkQuery();
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
			} else {
				includeDatabases.add(nsStr);
			}
		}
		initializeChunkQuery();
	}

	private Document initializeChunkQuery() {
		if (getIncludeNamespaces().size() > 0 || getIncludeDatabases().size() > 0) {
			List<Object> inList = new ArrayList<>();
			List<Object> orList = new ArrayList<>();
			// Document orDoc = new Document("$or", orList);
			chunkQuery.append("$or", orList);
			Document inDoc = new Document("ns", new Document("$in", inList));
			orList.add(inDoc);
			// orDoc.append("ns", inDoc);
			for (Namespace includeNs : getIncludeNamespaces()) {
				inList.add(includeNs.getNamespace());
			}
			for (String dbName : getIncludeDatabases()) {
				orList.add(regex("ns", "^" + dbName + "\\."));
			}
		} else {
			chunkQuery.append("ns", new Document("$ne", "config.system.sessions"));
		}
		return chunkQuery;
	}

	public boolean isFiltered() {
		return filtered;
	}

	public Document getChunkQuery() {
		if (chunkQuery.isEmpty()) {
			chunkQuery.append("ns", new Document("$ne", "config.system.sessions"));
		}
		return chunkQuery;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

	public double getSampleRate() {
		return sampleRate;
	}

	public void setSampleRate(double sampleRate) {
		this.sampleRate = sampleRate;
	}

	public int getSampleMinDocs() {
		return sampleMinDocs;
	}

	public void setSampleMinDocs(int sampleMinDocs) {
		this.sampleMinDocs = sampleMinDocs;
	}

	public int getMaxDocsToSamplePerPartition() {
		return maxDocsToSamplePerPartition;
	}

	public void setMaxDocsToSamplePerPartition(int maxDocsToSamplePerPartition) {
		this.maxDocsToSamplePerPartition = maxDocsToSamplePerPartition;
	}

	public int getMaxRetries() {
		return maxRetries;
	}

	public void setMaxRetries(int maxRetries) {
		this.maxRetries = maxRetries;
	}

	public long getDefaultPartitionSize() {
		return defaultPartitionSize;
	}

	public void setDefaultPartitionSize(long defaultPartitionSize) {
		this.defaultPartitionSize = defaultPartitionSize;
	}

	public boolean isUseStatusDb() {
		return useStatusDb;
	}

	public void setUseStatusDb(boolean useStatusDb) {
		this.useStatusDb = useStatusDb;
	}

	public String getStatusDbUri() {
		return statusDbUri;
	}

	public void setStatusDbUri(String statusDbUri) {
		this.statusDbUri = statusDbUri;
	}

	public String getStatusDbName() {
		return statusDbName;
	}

	public void setStatusDbName(String statusDbName) {
		this.statusDbName = statusDbName;
	}

	public String getStatusDbCollName() {
		return statusDbCollName;
	}

	public void setStatusDbCollName(String statusDbCollName) {
		this.statusDbCollName = statusDbCollName;
	}
}
