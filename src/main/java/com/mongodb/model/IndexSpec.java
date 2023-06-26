package com.mongodb.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

public class IndexSpec {
	
	public final static Set<String> validIndexOptions = new HashSet<>(
			Arrays.asList("2dsphereIndexVersion", "background", "bits", "bucketSize", "coarsestIndexedLevel", "collation", 
					"default_language", "expireAfterSeconds", "finestIndexedLevel", "hidden", "key", "language_override", 
					"max", "min", "name", "ns", "partialFilterExpression", "sparse", "storageEngine", "textIndexVersion", 
					"unique", "v", "weights", "wildcardProjection"));
	
	private RawBsonDocument sourceSpec;
	
	private RawBsonDocument key;
	private String keyJsonString;
	private String name;
	private Namespace namespace;
	private boolean sparse;
	private boolean background;
	private boolean unique;
	private Number expireAfterSeconds;
	
	private IndexSpec(RawBsonDocument sourceSpec, Namespace ns) {
		this.sourceSpec = sourceSpec;
		this.key = (RawBsonDocument)sourceSpec.get("key");
		this.keyJsonString = key.toJson();
		this.name = sourceSpec.getString("name").getValue();
		this.namespace = ns;
		if (sourceSpec.containsKey("expireAfterSeconds")) {
			this.expireAfterSeconds = sourceSpec.getNumber("expireAfterSeconds").doubleValue();
		}
		
		this.sparse = getBoolean(sourceSpec, "sparse");
		this.background = getBoolean(sourceSpec, "background");
		this.unique = getBoolean(sourceSpec, "unique");
	}
	
	private static boolean getBoolean(RawBsonDocument sourceSpec, String key) {
		BsonValue value = sourceSpec.get(key);
		if (value != null) {
			BsonType type = value.getBsonType();
			switch(type) {
            case BOOLEAN:
              return sourceSpec.getBoolean(key).getValue();
            case DOUBLE:
            	double d = sourceSpec.getDouble(key).getValue();
            	return !(d == 0.0);
            case INT32:
            	int i = sourceSpec.getInt32(key).getValue();
            	return !(i == 0);
            case INT64:
            	long l = sourceSpec.getInt64(key).getValue();
            	return !(l == 0L);
            default:
              return false;
            }
		}
		return false;
	}
	
	public static IndexSpec fromDocument(RawBsonDocument sourceSpec, Namespace ns) {
		IndexSpec spec = new IndexSpec(sourceSpec, ns);
		return spec;
	}
	
	public RawBsonDocument getSourceSpec() {
		return sourceSpec;
	}

	@Override
	public int hashCode() {
		return Objects.hash(key, namespace, expireAfterSeconds, sparse, unique);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexSpec other = (IndexSpec) obj;
		return  Objects.equals(namespace, other.namespace)
				&& Objects.equals(key, other.key)
				&& Objects.equals(expireAfterSeconds, other.expireAfterSeconds)
				&& sparse == other.sparse && unique == other.unique;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("IndexSpec [key=");
		builder.append(key);
		builder.append(", name=");
		builder.append(name);
		builder.append(", namespace=");
		builder.append(namespace);
		builder.append(", sparse=");
		builder.append(sparse);
		builder.append(", background=");
		builder.append(background);
		builder.append(", unique=");
		builder.append(unique);
		builder.append(", expireAfterSeconds=");
		builder.append(expireAfterSeconds);
		builder.append("]");
		return builder.toString();
	}

	public Number getExpireAfterSeconds() {
		return expireAfterSeconds;
	}

	public RawBsonDocument getKey() {
		return key;
	}

	public String getKeyJsonString() {
		return keyJsonString;
	}

	public String getName() {
		return name;
	}

	public Namespace getNamespace() {
		return namespace;
	}

	public boolean isSparse() {
		return sparse;
	}

	public boolean isBackground() {
		return background;
	}

	public boolean isUnique() {
		return unique;
	}
	
	

}
