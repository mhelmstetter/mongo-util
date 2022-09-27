package com.mongodb.model;


public class Resource {
	
    private String db;

    private String collection;

    private Boolean cluster;

    private Boolean anyResource;

    public String getDb() {
      return db;
    }

    public String getCollection() {
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
    
    

}
