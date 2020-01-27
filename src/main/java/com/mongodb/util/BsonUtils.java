package com.mongodb.util;

public class BsonUtils {
    
    public static long getEpochFromBsonTimestamp(long bsonTimestampLong) {
        return bsonTimestampLong >> 32;
    }

}
