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
	private Set<String> excludedCollections;
	private Map<String, Namespace> namespaces;

	public Database(String name, DatabaseStats dbStats) {
		this.name = name;
		this.dbStats = dbStats;
		allCollections = new HashMap<>();
		namespaces = new HashMap<>();
		shardedCollections = new HashSet<>();
		unshardedCollections = new HashSet<>();
		excludedCollections = new HashSet<>();
	}

	public void addCollection(Collection coll) {
		allCollections.put(coll.getNamespace(), coll);
		if (coll.isSharded()) {
			shardedCollections.add(coll);
		} else {
			unshardedCollections.add(coll);
		}
		Namespace ns = new Namespace(coll.getNamespace());
		namespaces.put(ns.getNamespace(), ns);
	}

	public void excludeCollection(String collNs) {
		excludedCollections.add(collNs);
	}

	public String getName() {
		return name;
	}

	public DatabaseStats getDbStats() {
		return dbStats;
	}

	public Set<Collection> getShardedCollections() {
		return shardedCollections == null ? new HashSet<>() : shardedCollections;
	}

	public Set<Collection> getUnshardedCollections() {
		return unshardedCollections == null ? new HashSet<>() : unshardedCollections;
	}
	public Set<Collection> getAllCollections() {
		return new HashSet<>(allCollections.values());
	}

	public Collection getCollection(String collName) {
		return allCollections.get(collName);
	}

	public java.util.Collection<Namespace> getNamespaces() {
		return namespaces.values();
	}

}
