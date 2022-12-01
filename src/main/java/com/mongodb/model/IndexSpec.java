package com.mongodb.model;

import java.util.Objects;

import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

public class IndexSpec {
	
	private RawBsonDocument sourceSpec;
	
	private RawBsonDocument key;
	private String keyJsonString;
	private String name;
	private Namespace namespace;
	private boolean sparse;
	private boolean background;
	private boolean unique;
	private Number expireAfterSeconds;
	
	private IndexSpec(RawBsonDocument sourceSpec) {
		this.sourceSpec = sourceSpec;
		this.key = (RawBsonDocument)sourceSpec.get("key");
		this.keyJsonString = key.toJson();
		this.name = sourceSpec.getString("name").getValue();
		if (sourceSpec.containsKey("ns")) {
			this.namespace = new Namespace(sourceSpec.getString("ns").getValue());
		}
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
	
	public static IndexSpec fromDocument(RawBsonDocument sourceSpec) {
		IndexSpec spec = new IndexSpec(sourceSpec);
		return spec;
	}
	


	@Override
	public String toString() {
		return sourceSpec.toJson();
	}

	

	public RawBsonDocument getSourceSpec() {
		return sourceSpec;
	}

	@Override
	public int hashCode() {
		return Objects.hash(background, expireAfterSeconds, key, name, namespace, sourceSpec, sparse, unique);
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
		return background == other.background && Objects.equals(expireAfterSeconds, other.expireAfterSeconds)
				&& Objects.equals(key, other.key) && Objects.equals(name, other.name)
				&& Objects.equals(namespace, other.namespace) && Objects.equals(sourceSpec, other.sourceSpec)
				&& sparse == other.sparse && unique == other.unique;
	}
	
	

}
