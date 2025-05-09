package com.mongodb.mongosync;

import com.mongodb.util.bson.BsonValueWrapper;

public class ShardKeyRange {
	final BsonValueWrapper min;
	final BsonValueWrapper max;
	
	public ShardKeyRange(BsonValueWrapper min, BsonValueWrapper max) {
		this.min = min;
		this.max = max;
	}
	
	public boolean overlaps(ShardKeyRange other) {
		// Check if this range overlaps with the other range
		return (min.compareTo(other.max) < 0 && max.compareTo(other.min) > 0);
	}
	
	@Override
	public String toString() {
		return "[" + min + " to " + max + "]";
	}
}