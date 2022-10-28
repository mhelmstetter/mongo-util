package com.mongodb.atlas.model;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

@JsonInclude(Include.NON_NULL)
public class AtlasResource {

	private String db;

	private String collection;
	
	private Boolean cluster;
	
	public AtlasResource() {
		super();
	}
	
	public AtlasResource(String db, String collection) {
		super();
		this.db = db;
		this.collection = collection;
	}

	public AtlasResource(String db, String collection, Boolean cluster) {
		super();
		this.db = db;
		this.collection = collection;
		this.cluster = cluster;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	@Override
	public String toString() {
		return "AtlasResource [db=" + db + ", collection=" + collection + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(collection, db);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AtlasResource other = (AtlasResource) obj;
		return Objects.equals(collection, other.collection) && Objects.equals(db, other.db);
	}

	public Boolean isCluster() {
		return cluster;
	}

	public void setCluster(Boolean cluster) {
		this.cluster = cluster;
	}
	
	

}
