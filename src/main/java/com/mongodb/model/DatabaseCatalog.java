package com.mongodb.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatabaseCatalog {
	
	private Map<String, Database> databases;

	private Set<Collection> shardedCollections;
	private Set<Collection> unshardedCollections;
	
	private long totalDocumentCount = 0L;
	private long totalDataSize = 0L;

	public DatabaseCatalog() {
		databases = new HashMap<>();
		shardedCollections = new HashSet<>();
		unshardedCollections = new HashSet<>();
	}

	public void addDatabase(Database db) {
		databases.put(db.getName(), db);
		totalDocumentCount += db.getTotalDocumentCount();
		totalDataSize += db.getTotalSize();
		shardedCollections.addAll(db.getShardedCollections());
		unshardedCollections.addAll(db.getUnshardedCollections());
	}

	public long[] getTotalSizeAndCount() {
		return new long[] {totalDataSize, totalDocumentCount};
	}

	public Set<Collection> getShardedCollections() {
		return shardedCollections;
	}

	public Set<Collection> getUnshardedCollections() {
		return unshardedCollections;
	}

	public Database getDatabase(String dbName) {
		return databases.get(dbName);
	}

}
