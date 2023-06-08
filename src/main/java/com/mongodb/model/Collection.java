package com.mongodb.model;

import java.util.Objects;
import java.util.Set;

public class Collection {
	
	private Namespace namespace;
	private boolean sharded;
	private CollectionStats collectionStats;
	private Set<IndexSpec> indexes;

	public Collection(Namespace namespace, boolean sharded, CollectionStats collectionStats, Set<IndexSpec> indexes) {
		this.namespace = namespace;
		this.sharded = sharded;
		this.collectionStats = collectionStats;
		this.indexes = indexes;
	}

	public Namespace getNamespace() {
		return namespace;
	}

	public boolean isSharded() {
		return sharded;
	}

	public CollectionStats getCollectionStats() {
		return collectionStats;
	}

	public Set<IndexSpec> getIndexes() {
		return indexes;
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

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Collection [namespace=");
		builder.append(namespace);
		builder.append(", sharded=");
		builder.append(sharded);
		builder.append("]");
		return builder.toString();
	}
	
	
}
