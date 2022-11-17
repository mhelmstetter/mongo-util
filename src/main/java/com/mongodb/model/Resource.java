package com.mongodb.model;

import java.util.Objects;

public class Resource {
	
    private String db;

    private String collection;

    private Boolean cluster;

    private Boolean anyResource;

    public String getDb() {
      return db;
    }

    public String getCollection() {
      if (Boolean.TRUE.equals(cluster) && collection == null) {
    	  return null;
      }
      return collection == null ? "" : collection;
    }

    public Boolean getCluster() {
      return cluster != null && cluster;
    }

    public Boolean getAnyResource() {
      return anyResource != null && anyResource;
    }

	@Override
	public String toString() {
		return "Resource [db=" + db + ", collection=" + collection + ", cluster=" + cluster + ", anyResource="
				+ anyResource + "]";
	}

	public void setDb(String db) {
		this.db = db;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public void setCluster(Boolean cluster) {
		this.cluster = cluster;
	}

	public void setAnyResource(Boolean anyResource) {
		this.anyResource = anyResource;
	}

	@Override
	public int hashCode() {
		return Objects.hash(anyResource, cluster, collection, db);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Resource other = (Resource) obj;
		return Objects.equals(anyResource, other.anyResource) && Objects.equals(cluster, other.cluster)
				&& Objects.equals(collection, other.collection) && Objects.equals(db, other.db);
	}
    
    

}
