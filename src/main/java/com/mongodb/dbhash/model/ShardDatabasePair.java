package com.mongodb.dbhash.model;

import com.mongodb.model.Namespace;

public class ShardDatabasePair {
	
	private final String sourceShard;
	private final String destinationShard;
	private final Namespace namespace;

	public ShardDatabasePair(String sourceShard, String destinationShard, Namespace ns) {
		this.sourceShard = sourceShard;
		this.destinationShard = destinationShard;
		this.namespace = ns;
	}

	public String getSourceShard() {
		return sourceShard;
	}

	public String getDestinationShard() {
		return destinationShard;
	}

	public Namespace getNamespace() {
		return namespace;
	}
}
