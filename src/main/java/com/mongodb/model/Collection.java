package com.mongodb.model;

public class Collection {
	
	private String namespace;
	private boolean sharded;

	public Collection(String namespace, boolean sharded) {
		this.namespace = namespace;
		this.sharded = sharded;
	}

	public String getNamespace() {
		return namespace;
	}

	public boolean isSharded() {
		return sharded;
	}
}
