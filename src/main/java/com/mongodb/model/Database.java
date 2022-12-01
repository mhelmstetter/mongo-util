package com.mongodb.model;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class Database {
	
	private String name;
	
	private Map<String, Collection> allCollections;
	private Set<Collection> shardedCollections;
	private Set<Collection> unshardedCollections;
	private Set<String> excludedCollections;
	private Map<String, Namespace> namespaces;
	private long totalDocumentCount;
	private long totalSize;

	public Database(String name) {
		this.name = name;
		allCollections = new HashMap<>();
		namespaces = new HashMap<>();
		shardedCollections = new HashSet<>();
		unshardedCollections = new HashSet<>();
		excludedCollections = new HashSet<>();
	}

	public void addCollection(Collection coll) {
		allCollections.put(coll.getNamespace().getNamespace(), coll);
		if (coll.isSharded()) {
			shardedCollections.add(coll);
		} else {
			unshardedCollections.add(coll);
		}
		Namespace ns = coll.getNamespace();
		namespaces.put(ns.getNamespace(), ns);
		totalDocumentCount += coll.getCollectionStats().getCount();
		totalSize += coll.getCollectionStats().getSize();
	}

	public void excludeCollection(String collNs) {
		excludedCollections.add(collNs);
	}

	public String getName() {
		return name;
	}

	public Set<Collection> getShardedCollections() {
		return shardedCollections;
	}

	public Set<Collection> getUnshardedCollections() {
		return unshardedCollections;
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

	public long getTotalDocumentCount() {
		return totalDocumentCount;
	}

	public long getTotalSize() {
		return totalSize;
	}

}
