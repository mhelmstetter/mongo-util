package com.mongodb.model;

import java.util.List;


public class Privilege {
	
    private Resource resource;

    private List<String> actions;

    public Resource getResource() {
      return resource;
    }

    public List<String> getActions() {
      return actions;
    }

	@Override
	public String toString() {
		return "Privilege [resource=" + resource + ", actions=" + actions + "]";
	}

	public void setResource(Resource resource) {
		this.resource = resource;
	}

	public void setActions(List<String> actions) {
		this.actions = actions;
	}
    
    

}
