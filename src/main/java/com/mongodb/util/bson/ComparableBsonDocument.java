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


    ComparableBsonDocument(BsonDocument document) {
        this.document = document;
    }

    @Override
    public int compareTo(ComparableBsonDocument other) {
        List<BsonValue> values1 = new ArrayList<>(this.document.values());
        List<BsonValue> values2 = new ArrayList<>(other.document.values());

        int minSize = Math.min(values1.size(), values2.size());
        for (int i = 0; i < minSize; i++) {
            BsonValue value1 = values1.get(i);
            BsonValue value2 = values2.get(i);

            int comparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(value2));
            if (comparison != 0) {
                return comparison;
            }
        }

        return Integer.compare(values1.size(), values2.size());
    }
}