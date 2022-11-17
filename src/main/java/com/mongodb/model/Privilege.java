package com.mongodb.model;

import java.util.List;
import java.util.Objects;


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

	@Override
	public int hashCode() {
		return Objects.hash(actions, resource);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Privilege other = (Privilege) obj;
		return Objects.equals(actions, other.actions) && Objects.equals(resource, other.resource);
	}
    
    

}
