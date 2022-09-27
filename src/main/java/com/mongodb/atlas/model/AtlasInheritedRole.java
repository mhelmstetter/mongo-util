package com.mongodb.atlas.model;

public class AtlasInheritedRole {

	private String db;
	private String role;
	
	

	public AtlasInheritedRole(String db, String role) {
		super();
		this.db = db;
		this.role = role;
	}

	public String getDb() {
		return db;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public String getRole() {
		return role;
	}

	public void setRole(String role) {
		this.role = role;
	}

	@Override
	public String toString() {
		return "AtlasInheritedRole [db=" + db + ", role=" + role + "]";
	}
	

}
