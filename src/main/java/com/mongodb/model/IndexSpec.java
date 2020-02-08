package com.mongodb.model;

import org.bson.BsonBoolean;
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
		this.namespace = new Namespace(sourceSpec.getString("ns").getValue());
		if (sourceSpec.containsKey("expireAfterSeconds")) {
			this.expireAfterSeconds = sourceSpec.getNumber("expireAfterSeconds").doubleValue();
		}
		
		this.sparse = sourceSpec.getBoolean("sparse", BsonBoolean.FALSE).getValue();
		this.background = sourceSpec.getBoolean("background", BsonBoolean.FALSE).getValue();
		this.unique = sourceSpec.getBoolean("unique", BsonBoolean.FALSE).getValue();
	}
	
	public static IndexSpec fromDocument(RawBsonDocument sourceSpec) {
		IndexSpec spec = new IndexSpec(sourceSpec);
		return spec;
	}

	@Override
	public int hashCode() {
		return keyJsonString.hashCode();
	}
	


	@Override
	public String toString() {
		return sourceSpec.toJson();
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
        return other.keyJsonString.equals(this.keyJsonString);
		
	}

	public RawBsonDocument getSourceSpec() {
		return sourceSpec;
	}
	
	

}
