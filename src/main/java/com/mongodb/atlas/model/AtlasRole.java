package com.mongodb.atlas.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonRootName;



@JsonRootName(value = "role")
public class AtlasRole {
	
	private String roleName;
	
	Map<String, AtlasRoleAction> actionsMap = new HashMap<>();
	
	private List<AtlasInheritedRole> inheritedRoles = new ArrayList<>();

	public String getRoleName() {
		return roleName;
	}

	public void setRoleName(String roleName) {
		this.roleName = roleName;
	}

	@JsonProperty("actions")
	public Collection<AtlasRoleAction> getActions() {
		return actionsMap.values();
	}
	
	@JsonProperty("actions")
	public void setActions(Collection<AtlasRoleAction> actions) {
		for (AtlasRoleAction a : actions) {
			actionsMap.put(a.getAction(), a);
		}
	}
	
	public void addAction(AtlasRoleAction a) {
		actionsMap.put(a.getAction(), a);
	}
	
	public AtlasRoleAction getAction(String roleName) {
		return actionsMap.get(roleName);
	}
	
	public void addInheritedRole(AtlasInheritedRole r) {
		inheritedRoles.add(r);
	}

	@Override
	public String toString() {
		return "AtlasRole [roleName=" + roleName + ", actions=" + actionsMap.values() +  ", inheritedRoles=" + inheritedRoles + "]";
	}

	public List<AtlasInheritedRole> getInheritedRoles() {
		return inheritedRoles;
	}



}
