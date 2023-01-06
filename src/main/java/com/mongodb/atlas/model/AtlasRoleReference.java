package com.mongodb.atlas.model;

import java.util.Objects;

public class AtlasRoleReference {

	public AtlasRoleReference(String roleName, String databaseName) {
		this.roleName = roleName;
		this.databaseName = databaseName;
	}
	
	public AtlasRoleReference() {
		
	}

	private String roleName;
	private String databaseName;

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	public String getDatabaseName() {
		return databaseName;
	}

	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	@Override
	public int hashCode() {
		return Objects.hash(databaseName, roleName);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		AtlasRoleReference other = (AtlasRoleReference) obj;
		return Objects.equals(databaseName, other.databaseName) && Objects.equals(roleName, other.roleName);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(roleName);
		builder.append("@");
		builder.append(databaseName);
		return builder.toString();
	}
	
	

}
