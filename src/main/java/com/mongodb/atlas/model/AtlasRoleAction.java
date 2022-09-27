package com.mongodb.atlas.model;

import java.util.ArrayList;
import java.util.List;

public class AtlasRoleAction {
	
	private String action;
	
	private List<AtlasResource> resources = new ArrayList<>();

	public String getAction() {
		return action;
	}

	public void setAction(String action) {
		this.action = action;
	}

	public List<AtlasResource> getResources() {
		return resources;
	}
	
	public void addResource(AtlasResource r) {
		resources.add(r);
	}

	public void setResources(List<AtlasResource> resources) {
		this.resources = resources;
	}

	@Override
	public String toString() {
		return "AtlasRoleAction [action=" + action + ", resources=" + resources + "]";
	}
	
	

}
