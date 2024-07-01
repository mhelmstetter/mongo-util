package com.mongodb.util.bson;

import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonString;

public class BsonUtils {
	
    /**
     * Converts a Set<String> to a BsonArray.
     *
     * @param stringSet the Set<String> to convert
     * @return the resulting BsonArray
     */
    public static BsonArray convertSetToBsonArray(Set<String> stringSet) {
        BsonArray bsonArray = new BsonArray();
        for (String str : stringSet) {
            bsonArray.add(new BsonString(str));
        }
        return bsonArray;
    }

}
