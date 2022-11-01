package com.mongodb.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Database {
	
	private String name;
	
	private DatabaseStats dbStats;
	
	private Map<String, Collection> allCollections;
	private Set<Collection> shardedCollections;
	private Set<Collection> unshardedCollections;
	private Map<String, Namespace> namespaces;

	public Database(String name, DatabaseStats dbStats) {
		this.name = name;
		this.dbStats = dbStats;
	}

	public void addCollection(Collection coll) {
		if (allCollections == null) {
			allCollections = new HashMap<>();
			namespaces = new HashMap<>();
			shardedCollections = new HashSet<>();
			unshardedCollections = new HashSet<>();
		}
		allCollections.put(coll.getNamespace(), coll);
		if (coll.isSharded()) {
			shardedCollections.add(coll);
		} else {
			unshardedCollections.add(coll);
		}
		Namespace ns = new Namespace(coll.getNamespace());
		namespaces.put(ns.getNamespace(), ns);
	}

	public String getName() {
		return name;
	}

	public DatabaseStats getDbStats() {
		return dbStats;
	}

	public Set<Collection> getShardedCollections() {
		return shardedCollections;
	}

	public Set<Collection> getUnshardedCollections() {
		return unshardedCollections;
	}

	public java.util.Collection<Namespace> getNamespaces() {
		return namespaces.values();
	}

}
