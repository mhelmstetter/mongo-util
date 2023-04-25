package com.mongodb.atlas.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.mongodb.model.Role;
import com.mongodb.model.User;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasUser {
	
	private String username;
	private String databaseName;
	private String password;
	
	private List<AtlasRoleReference> roles = new ArrayList<>();
	
	public AtlasUser() {
		
	}

	public AtlasUser(User u, String password) {
		setUsername(u.getUser());
		this.databaseName = "admin";
		this.password = password;
		for (Role r : u.getRoles()) {
			AtlasRoleReference ref = new AtlasRoleReference();
			ref.setDatabaseName(r.getDb());
			ref.setRoleName(r.getRole());
			roles.add(ref);
		}
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username.replaceAll("[\\.|-]", "_");
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public List<AtlasRoleReference> getRoles() {
		return roles;
	}

	public void setRoles(List<AtlasRoleReference> roles) {
		this.roles = roles;
	}
	
	public boolean hasRole(AtlasRoleReference ref) {
		return roles.contains(ref);
	}
	
	public boolean hasRole(String roleName, String databaseName) {
		AtlasRoleReference ref = new AtlasRoleReference();
		ref.setRoleName(roleName);
		ref.setDatabaseName(databaseName);
		return roles.contains(ref);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("AtlasUser [username=");
		builder.append(username);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		return Objects.hash(username);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AtlasUser other = (AtlasUser) obj;
		return Objects.equals(username, other.username);
	}

}
