package com.mongodb.util.bson;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

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
        } else if (value instanceof BsonDocument) {
            return (BsonDocument)value;
        }
        // Add more types as needed
        
        throw new IllegalArgumentException("Unsupported type: " + value.getClass().getName());
    }
    
    /**
     * Converts a Document to a BsonDocument
     * 
     * @param doc The Document to convert
     * @return The converted BsonDocument
     */
    public static BsonDocument convertToBsonDocument(Document doc) {
        if (doc == null) return null;
        
        BsonDocument bsonDoc = new BsonDocument();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            bsonDoc.append(key, convertToBsonValue(value));
        }
        return bsonDoc;
    }
    
    public static Object convertBsonValueToObject(BsonValue value) {
		if (value == null) return null;
		
		switch (value.getBsonType()) {
			case DOCUMENT:
				Document doc = new Document();
				BsonDocument bsonDoc = value.asDocument();
				for (String key : bsonDoc.keySet()) {
					doc.put(key, convertBsonValueToObject(bsonDoc.get(key)));
				}
				return doc;
			case ARRAY:
				List<Object> list = new ArrayList<>();
				for (BsonValue item : value.asArray()) {
					list.add(convertBsonValueToObject(item));
				}
				return list;
			case OBJECT_ID:
				return value.asObjectId().getValue();
			case STRING:
				return value.asString().getValue();
			case BOOLEAN:
				return value.asBoolean().getValue();
			case INT32:
				return value.asInt32().getValue();
			case INT64:
				return value.asInt64().getValue();
			case DOUBLE:
				return value.asDouble().getValue();
			case DECIMAL128:
				return value.asDecimal128().getValue();
			case DATE_TIME:
				return new Date(value.asDateTime().getValue());
			case NULL:
				return null;
			// Add other types as needed
			default:
				return value.toString();
		}
	}
}
