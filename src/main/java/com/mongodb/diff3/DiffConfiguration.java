package com.mongodb.diff3;

import java.util.Arrays;

import org.bson.Document;

import com.mongodb.shardsync.BaseConfiguration;

public class DiffConfiguration extends BaseConfiguration {
	
	public final static String PARTITION_MODE = "partition";
	public final static String SHARD_MODE = "shard";
	public final static String RECHECK_MODE = "recheck";
	
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
	private final String[] knownModes = new String[]{PARTITION_MODE, RECHECK_MODE, SHARD_MODE};

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		if (!(Arrays.binarySearch(knownModes, mode) >= 0)){
			throw new RuntimeException("Unknown mode: " + mode);
		}
		this.mode = mode;
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
