package com.mongodb.util;

import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

public class BsonUtils {
    
    public static long getEpochFromBsonTimestamp(long bsonTimestampLong) {
        return bsonTimestampLong >> 32;
    }
    
    public static BsonValue getValueFromString(String bsonTypeStr, String val) {
    	if (bsonTypeStr.equals("BsonString")) {
    		return new BsonString(val);
    	} else if (bsonTypeStr.equals("BsonInt64")) {
    		return new BsonInt64(Long.parseLong(val));
    	}
    	return null;
    }

}
