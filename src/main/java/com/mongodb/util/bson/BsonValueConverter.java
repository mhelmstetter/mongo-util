package com.mongodb.util.bson;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

public class BsonValueConverter {
	
    public static BsonValue convertToBsonValue(Object value) {
        if (value == null) {
            return new BsonNull();
        } else if (value instanceof String) {
            return new BsonString((String) value);
        } else if (value instanceof Integer) {
            return new BsonInt32((Integer) value);
        } else if (value instanceof Long) {
            return new BsonInt64((Long) value);
        } else if (value instanceof Double) {
            return new BsonDouble((Double) value);
        } else if (value instanceof Boolean) {
            return new BsonBoolean((Boolean) value);
        } else if (value instanceof ObjectId) {
            return new BsonObjectId((ObjectId) value);
        } else if (value instanceof Document) {
            return BsonDocument.parse(((Document) value).toJson());
        }
        // Add more types as needed
        
        throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
    }
}
