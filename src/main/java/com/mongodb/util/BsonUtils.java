package com.mongodb.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.model.Namespace;

public class BsonUtils {

	// {value='1'}
	private final static Pattern valuePattern = Pattern.compile("^\\{value='(.*)'\\}$");

	public static long getEpochFromBsonTimestamp(long bsonTimestampLong) {
		return bsonTimestampLong >> 32;
	}

	public static BsonValue getValueFromString(String bsonTypeStr, String val) {
    	
    	Matcher m = valuePattern.matcher(val);
        if (m.find()) {
        	String s1 = m.group(1);
        	if (bsonTypeStr.equals("BsonString")) {
        		return new BsonString(s1);
        	} else if (bsonTypeStr.equals("BsonInt64")) {
        		return new BsonInt64(Long.parseLong(val));
        	} else if (val.startsWith("{")) {
        		return BsonDocument.parse(val);
        	}
        } else {
        	if (bsonTypeStr.equals("BsonObject")) {
        		return BsonDocument.parse(val);
        	}
        }
    	return null;
    }

}
