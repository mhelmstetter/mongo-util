package com.mongodb.model;

public class Collection {
	
	private String namespace;
	private CollectionStats collStats;

	public Collection(String namespace, CollectionStats collStats) {
		this.namespace = namespace;
		this.collStats = collStats;
	}

	public String getNamespace() {
		return namespace;
	}

	public CollectionStats getCollStats() {
		return collStats;
	}

	public boolean isSharded() {
		return collStats.isSharded();
	}
}
