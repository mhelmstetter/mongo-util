package com.mongodb.model;

public class Collection {
	
	private Namespace namespace;
	private boolean sharded;

	public Collection(Namespace namespace, boolean sharded) {
		this.namespace = namespace;
		this.sharded = sharded;
	}

	public Namespace getNamespace() {
		return namespace;
	}

	public boolean isSharded() {
		return sharded;
	}
}
