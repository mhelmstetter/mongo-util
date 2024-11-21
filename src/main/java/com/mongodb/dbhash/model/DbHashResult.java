package com.mongodb.dbhash.model;

import com.mongodb.model.Namespace;

public class DbHashResult {

	String sourceShard;
	String destShard;
	Namespace namespace;
	String hash;
	boolean isSource;

	public DbHashResult(Namespace ns, String hash, boolean isSource) {
		this.namespace = ns;
		this.hash = hash;
		this.isSource = isSource;
	}
	
	public String getShardPairKey() {
		return sourceShard + "|" + destShard;
	}

	public String getHash() {
		return hash;
	}

	public void setHash(String hash) {
		this.hash = hash;
	}

	

	public Namespace getNamespace() {
		return namespace;
	}



	public String getSourceShard() {
		return sourceShard;
	}



	public void setSourceShard(String sourceShard) {
		this.sourceShard = sourceShard;
	}



	public String getDestShard() {
		return destShard;
	}



	public void setDestShard(String destShard) {
		this.destShard = destShard;
	}

	public boolean isSource() {
		return isSource;
	}
}
