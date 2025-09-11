package com.mongodb.util.bson;

import java.util.ArrayList;
import java.util.List;

import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonValue;

public class ComparableBsonDocument implements Comparable<ComparableBsonDocument> {
    private final BsonDocument document;
    
    private final static BsonMinKey min = new BsonMinKey();
    private final static BsonMaxKey max = new BsonMaxKey();


    public ComparableBsonDocument(BsonDocument document) {
        this.document = document;
    }

    @Override
    public int compareTo(ComparableBsonDocument other) {
        // Get all keys from both documents and sort them
        List<String> keys1 = new ArrayList<>(this.document.keySet());
        List<String> keys2 = new ArrayList<>(other.document.keySet());
        keys1.sort(String::compareTo);
        keys2.sort(String::compareTo);

        // Debug logging
        System.out.println("DEBUG ComparableBsonDocument: Comparing " + this.document + " vs " + other.document);
        System.out.println("DEBUG keys1: " + keys1 + ", keys2: " + keys2);

        // Compare by iterating through keys in sorted order
        int minSize = Math.min(keys1.size(), keys2.size());
        for (int i = 0; i < minSize; i++) {
            String key1 = keys1.get(i);
            String key2 = keys2.get(i);
            
            // First compare the keys themselves
            int keyComparison = key1.compareTo(key2);
            System.out.println("DEBUG key comparison: '" + key1 + "' vs '" + key2 + "' = " + keyComparison);
            if (keyComparison != 0) {
                return keyComparison;
            }
            
            // Keys are equal, compare the values
            BsonValue value1 = this.document.get(key1);
            BsonValue value2 = other.document.get(key2);
            
            System.out.println("DEBUG value comparison: " + value1 + " vs " + value2);
            int valueComparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(value2));
            System.out.println("DEBUG value comparison result: " + valueComparison);
            if (valueComparison != 0) {
                return valueComparison;
            }
        }

        // All compared key-value pairs are equal, compare by number of keys
        int result = Integer.compare(keys1.size(), keys2.size());
        System.out.println("DEBUG final size comparison: " + result);
        return result;
    }
}