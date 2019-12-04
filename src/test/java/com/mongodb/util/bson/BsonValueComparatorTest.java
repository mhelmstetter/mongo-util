package com.mongodb.util.bson;

import static org.junit.Assert.assertEquals;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.Test;

public class BsonValueComparatorTest {
    
    private final static BsonValueComparator comparator = new BsonValueComparator();

    @Test
    public void testEqualBsonStrings() {
        BsonString x = new BsonString("abc");
        BsonString y = new BsonString("abc");
        int compareResult = comparator.compare(x, y);
        assertEquals(0, compareResult);
    }
    
    @Test
    public void testAscendingBsonStrings() {
        BsonString x = new BsonString("a");
        BsonString y = new BsonString("b");
        int compareResult = comparator.compare(x, y);
        assertEquals(-1, compareResult);
    }
    
    @Test
    public void testDescendingBsonStrings() {
        BsonString x = new BsonString("b");
        BsonString y = new BsonString("a");
        int compareResult = comparator.compare(x, y);
        assertEquals(1, compareResult);
    }
    
    @Test
    public void testCompareDocuments() {
        BsonDocument d1 = new BsonDocument()
                .append("x", new BsonString("aaa"))
                .append("y", new BsonString("bbb"));
        
        BsonDocument d2 = new BsonDocument()
                .append("x", new BsonString("aaa"))
                .append("y", new BsonString("bbb"));
        
        BsonDocument raw1 = new RawBsonDocument(d1, new BsonDocumentCodec());
        BsonDocument raw2 = new RawBsonDocument(d2, new BsonDocumentCodec());
                

        int compareResult = comparator.compare(raw1, raw2);
        assertEquals(0, compareResult);
    }

}
