package com.mongodb.util.bson;

import java.util.Objects;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

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
				compareDocs((BsonDocument) this.value, (BsonDocument) other.value);
				break;
			case MIN_KEY:
				return 0;
			default:
				throw new IllegalArgumentException("BsonValueWrapper not implemented for type " + thisType.toString());
			}
		}
		
		if (otherType.equals(BsonType.MIN_KEY)) {
			return 1;
		} else {
			System.out.println("comparing " + thisType + " to " + otherType + " - " + this.getValue() + " to " + other.getValue());
			return 0;
		}
	}

	public int compareDocs(BsonDocument x, BsonDocument y) {
		String s1 = x.toJson();
		String s2 = y.toJson();
		return s1.compareTo(s2);
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
