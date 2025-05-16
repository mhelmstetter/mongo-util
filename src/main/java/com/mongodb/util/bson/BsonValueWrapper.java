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

	    // Special case for numeric types to avoid precision issues
	    if (isNumeric(this.value) && isNumeric(other.value)) {
	        if (this.value.isInt32() && other.value.isInt32()) {
	            // Compare as integers for better precision
	            int thisInt = this.value.asInt32().getValue();
	            int otherInt = other.value.asInt32().getValue();
	            return Integer.compare(thisInt, otherInt);
	        } else if (this.value.isInt64() && other.value.isInt64()) {
	            // Compare as longs for better precision
	            long thisLong = this.value.asInt64().getValue();
	            long otherLong = other.value.asInt64().getValue();
	            return Long.compare(thisLong, otherLong);
	        } else {
	            // Fallback to double comparison for mixed numeric types
	            double thisDouble = getNumericValue(this.value);
	            double otherDouble = getNumericValue(other.value);
	            return Double.compare(thisDouble, otherDouble);
	        }
	    }

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
	        return 0;
	    }
	}

	// Helper method to check if a BsonValue is a numeric type
	private boolean isNumeric(BsonValue value) {
	    return value.isInt32() || value.isInt64() || value.isDouble() || value.isDecimal128();
	}

	// Helper method to extract a numeric value as double
	private double getNumericValue(BsonValue value) {
	    if (value.isInt32()) {
	        return value.asInt32().getValue();
	    } else if (value.isInt64()) {
	        return value.asInt64().getValue();
	    } else if (value.isDouble()) {
	        return value.asDouble().getValue();
	    } else if (value.isDecimal128()) {
	        return value.asDecimal128().getValue().doubleValue();
	    }
	    throw new IllegalArgumentException("Not a numeric BsonValue: " + value.getBsonType());
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
