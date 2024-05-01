package com.mongodb.util.bson;

import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;
import java.util.TreeMap;

public class ComparableBsonDocument implements Comparable<ComparableBsonDocument> {
    private final BsonDocument document;

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

            if (value2 == null) {
                return 1; // key exists in this document but not in the other
            }

            int comparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(value2));
            if (comparison != 0) {
                return comparison;
            }
        }

        for (String key : sortedMap2.keySet()) {
            if (!sortedMap1.containsKey(key)) {
                return -1; // key exists in the other document but not in this
            }
        }

        return 0; // documents are equal
    }
}