package com.mongodb.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DatabaseCatalog {
	
	private Map<String, Database> databases;
	
	private Long documentCount = 0L;
	private Set<Collection> shardedCollections;
	private Set<Collection> unshardedCollections;

	public void addDatabase(Database db) {
		if (this.databases == null) {
			databases = new HashMap<>();
		}
		if (this.shardedCollections == null) {
			shardedCollections = new HashSet<>();
		}
		if (this.unshardedCollections == null) {
			unshardedCollections = new HashSet<>();
		}
		databases.put(db.getName(), db);
		documentCount += db.getDbStats().getDocumentCount();

		shardedCollections.addAll(db.getShardedCollections());
		unshardedCollections.addAll(db.getUnshardedCollections());
	}

	public long getTotalSize() {
		long sum = 0;
		for (Database db : databases.values()) {
			for (Collection coll : db.getAllCollections()) {
				sum += coll.getCollStats().getSize();
			}
		}
		return sum;
	}

	public Long getDocumentCount() {
		return documentCount;
	}

	public Set<Collection> getShardedCollections() {
		return shardedCollections;
	}

	public Set<Collection> getUnshardedCollections() {
		return unshardedCollections;
	}

}
