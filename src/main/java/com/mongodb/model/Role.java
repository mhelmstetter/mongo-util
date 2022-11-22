package com.mongodb.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Role {

	private String id;

	private String role;

	private String db;

	private Boolean isBuiltin;

	private List<Privilege> privileges;

	private List<Role> roles;

	public String getRole() {
		return role;
	}

	public String getDb() {
		return db;
	}

	public Boolean getIsBuiltin() {
		return isBuiltin;
	}

	public List<Role> getRoles() {
		return roles == null ? Collections.emptyList() : roles;
	}

	public List<Privilege> getPrivileges() {
		return privileges == null ? Collections.emptyList() : privileges;
	}
	
	public Set<Privilege> getResoucePrivilegeSet() {
		return Set.copyOf(privileges);
	}

	@Override
	public String toString() {
		return "Role [role=" + role + ", db=" + db + ", isBuiltin=" + isBuiltin + ", privileges=" + privileges
				+ ", roles=" + roles + "]";
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public void setRole(String role) {
		this.role = role;
	}

	public void setDb(String db) {
		this.db = db;
	}

	public void setIsBuiltin(Boolean isBuiltin) {
		this.isBuiltin = isBuiltin;
	}

	public void setPrivileges(List<Privilege> privileges) {
		this.privileges = privileges;
	}

	public void setRoles(List<Role> roles) {
		this.roles = roles;
	}

}