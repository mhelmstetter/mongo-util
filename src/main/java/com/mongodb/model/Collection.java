package com.mongodb.model;

import java.util.Objects;

public class Collection {
	
	private String namespace;
	private boolean sharded;

	public Collection(String namespace, boolean sharded) {
		this.namespace = namespace;
		this.sharded = sharded;
	}

	public String getNamespace() {
		return namespace;
	}

	public boolean isSharded() {
		return sharded;
	}

	@Override
	public int hashCode() {
		return Objects.hash(namespace, sharded);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Collection other = (Collection) obj;
		return Objects.equals(namespace, other.namespace) && sharded == other.sharded;
	}
	
	
}
