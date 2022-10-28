package com.mongodb.atlas.model;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class AtlasRoleAction {
	
	private String action;
	
	private Set<AtlasResource> resources = new HashSet<>();

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public Collection<AtlasResource> getResources() {
		return resources;
	}
	
	public void addResource(AtlasResource r) {
		resources.add(r);
	}

//	public void setResources(List<AtlasResource> resources) {
//		this.resources = resources;
//	}

	@Override
	public String toString() {
		return "AtlasRoleAction [action=" + action + ", resources=" + resources + "]";
	}
	
	

}
