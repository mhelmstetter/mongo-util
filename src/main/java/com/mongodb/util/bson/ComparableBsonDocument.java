package com.mongodb.util.bson;

import java.util.Map;
import java.util.TreeMap;

import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;

public class ComparableBsonDocument implements Comparable<ComparableBsonDocument> {
    private final BsonDocument document;
    
    private final static BsonMinKey min = new BsonMinKey();
    private final static BsonMaxKey max = new BsonMaxKey();


    ComparableBsonDocument(BsonDocument document) {
        this.document = document;
    }

    @Override
    public int compareTo(ComparableBsonDocument other) {
        Map<String, BsonValue> sortedMap1 = new TreeMap<>(this.document);
        Map<String, BsonValue> sortedMap2 = new TreeMap<>(other.document);

        for (Map.Entry<String, BsonValue> entry1 : sortedMap1.entrySet()) {
            String key = entry1.getKey();
            BsonValue value1 = entry1.getValue();
            BsonValue value2 = sortedMap2.get(key);

            int comparison;
            if (value2 == null) {
                comparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(min));
            } else {
                comparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(value2));
            }

            if (comparison != 0) {
                return comparison;
            }
        }

        for (String key : sortedMap2.keySet()) {
            if (!sortedMap1.containsKey(key)) {
                BsonValue value2 = sortedMap2.get(key);
                int comparison = new BsonValueWrapper(max).compareTo(new BsonValueWrapper(value2));
                if (comparison != 0) {
                    return comparison;
                }
            }
        }

        return 0; // documents are equal
    }
}