package com.mongodb.util.bson;

import java.util.Objects;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;

public class BsonValueWrapper implements Comparable<BsonValueWrapper> {

	private BsonValue value;

	public BsonValueWrapper(BsonValue value) {
		if (value == null) {
			throw new IllegalArgumentException("Value can not be null");
		}
		this.value = value;
	}

	@Override
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public int compareTo(BsonValueWrapper other) {
		
		BsonType thisType = this.value.getBsonType();
		BsonType otherType = other.value.getBsonType();

		if (thisType.equals(otherType)) {

			if (this.value instanceof Comparable) {
				return ((Comparable) this.value).compareTo((Comparable) other.value);
			}

			switch (thisType) {
			case DOCUMENT:
				return compareDocs((BsonDocument) this.value, (BsonDocument) other.value);
			case MIN_KEY:
				return 0;
			default:
				throw new IllegalArgumentException("BsonValueWrapper not implemented for type " + thisType.toString());
			}
		}
		
		if (otherType.equals(BsonType.MIN_KEY)) {
			return 1;
		} else {
			//System.out.println("*** comparing " + thisType + " to " + otherType + " - " + this.getValue() + " to " + other.getValue());
			return 0;
		}
	}

	public int compareDocs(BsonDocument x, BsonDocument y) {
		int result;
		String s1 = x.toJson();
		String s2 = y.toJson();
		if (s2.contains("{\"$minKey\": 1}")) {
			result = 1;
		} else {
			result = s1.compareTo(s2);
		}
		
		//System.out.println("compare: " + x + " --> " + y + " **result: " + result);
		return result;
	}
	
	private Document bsonValueWrapperToDocument(BsonValueWrapper wrapper, Set<String> fieldNames) {
		BsonValue bsonValue = wrapper.getValue();
		
		if (bsonValue instanceof BsonDocument) {
			// For compound shard keys
			BsonDocument doc = (BsonDocument) bsonValue;
			Document result = new Document();
			
			for (String key : doc.keySet()) {
				result.put(key, BsonValueConverter.convertBsonValueToObject(doc.get(key)));
			}
			
			return result;
		} else if (fieldNames.size() == 1) {
			// For single-field shard key
			String fieldName = fieldNames.iterator().next();
			return new Document(fieldName, BsonValueConverter.convertBsonValueToObject(bsonValue));
		} else {
			throw new IllegalArgumentException(
				"Cannot convert non-document BsonValue to Document with multiple field names");
		}
	}

	public BsonValue getValue() {
		return value;
	}

	@Override
	public int hashCode() {
		return Objects.hash(value);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		BsonValueWrapper other = (BsonValueWrapper) obj;
		return Objects.equals(value, other.value);
	}

	@Override
	public String toString() {
		return value.toString();
	}

}
