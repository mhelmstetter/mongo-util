package com.mongodb.model;

import java.util.Collections;
import java.util.List;

public class User {

   

    private String id;

    private String user;

    private String db;

    private List<Role> roles;

    public String getId() {
      return id;
    }

    public String getUser() {
      return user;
    }

    public String getDb() {
      return db;
    }

    public List<Role> getRoles() {
      return roles == null ? Collections.emptyList() : roles;
    }

	public void setId(String id) {
		this.id = id;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public void setRoles(List<Role> roles) {
		this.roles = roles;
	}

    
}