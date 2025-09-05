package com.mongodb.util.bson;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonString;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;

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

    @Test
    public void testNumericTypeDifferences_Int32VsInt64() {
        // Test the same value represented as different numeric types
        BsonInt32 int32Value = new BsonInt32(1);
        BsonInt64 int64Value = new BsonInt64(1L);
        
        // These represent the same logical value (1) but with different BSON types
        int compareResult = comparator.compare(int32Value, int64Value);
        
        // BsonValueComparator correctly treats these as equal even though they have different BSON types
        // This is the correct behavior for semantic comparison
        assertEquals(0, compareResult, 
            "BsonValueComparator correctly treats BsonInt32(1) and BsonInt64(1) as equal values " +
            "despite different BSON types, because they're mathematically equivalent");
    }
    
    @Test
    public void testNumericTypeDifferences_Int32VsDouble() {
        // Test integer vs double representation of the same value
        BsonInt32 intValue = new BsonInt32(1);
        BsonDouble doubleValue = new BsonDouble(1.0);
        
        // These represent the same logical value (1) but with different BSON types
        int compareResult = comparator.compare(intValue, doubleValue);
        
        // BsonValueComparator correctly treats these as equal even though they have different BSON types
        assertEquals(0, compareResult, 
            "BsonValueComparator correctly treats BsonInt32(1) and BsonDouble(1.0) as equal values " +
            "despite different BSON types, because they're mathematically equivalent");
    }
    
    @Test
    public void testNumericTypeDifferences_Int64VsDouble() {
        // Test long vs double representation of the same value
        BsonInt64 longValue = new BsonInt64(1L);
        BsonDouble doubleValue = new BsonDouble(1.0);
        
        // These represent the same logical value (1) but with different BSON types
        int compareResult = comparator.compare(longValue, doubleValue);
        
        // BsonValueComparator correctly treats these as equal even though they have different BSON types
        assertEquals(0, compareResult, 
            "BsonValueComparator correctly treats BsonInt64(1) and BsonDouble(1.0) as equal values " +
            "despite different BSON types, because they're mathematically equivalent");
    }
    
    @Test
    public void testNumericTypeDifferences_IndexKeyComparison() {
        // Test the specific case that affects index key comparisons
        // This simulates what happens when comparing index keys like {x: 1} with different numeric types
        
        BsonDocument indexKey1 = new BsonDocument("x", new BsonInt64(1L));  // NumberLong(1)
        BsonDocument indexKey2 = new BsonDocument("x", new BsonInt32(1));   // NumberInt(1)
        
        // Convert to RawBsonDocument to simulate how index specs are stored/compared
        RawBsonDocument rawKey1 = new RawBsonDocument(indexKey1, new BsonDocumentCodec());
        RawBsonDocument rawKey2 = new RawBsonDocument(indexKey2, new BsonDocumentCodec());
        
        int compareResult = comparator.compare(rawKey1, rawKey2);
        
        // BsonValueComparator correctly treats these documents as equal
        // This means the index comparison bug is NOT in BsonValueComparator itself
        assertEquals(0, compareResult,
            "BsonValueComparator correctly treats index keys {x: NumberLong(1)} and {x: NumberInt(1)} " +
            "as equal. This proves the index comparison bug is NOT in BsonValueComparator - the bug " +
            "must be in IndexSpec.equals() or findDestinationIndex() which use direct object equality " +
            "instead of semantic comparison via BsonValueComparator.");
    }
    
    @Test
    public void testSameNumericTypes_ShouldBeEqual() {
        // Verify that same numeric types with same values are correctly identified as equal
        BsonInt32 int1 = new BsonInt32(1);
        BsonInt32 int2 = new BsonInt32(1);
        
        int compareResult = comparator.compare(int1, int2);
        assertEquals(0, compareResult, "Same BSON types with same values should be equal");
        
        BsonInt64 long1 = new BsonInt64(1L);
        BsonInt64 long2 = new BsonInt64(1L);
        
        compareResult = comparator.compare(long1, long2);
        assertEquals(0, compareResult, "Same BSON types with same values should be equal");
        
        BsonDouble double1 = new BsonDouble(1.0);
        BsonDouble double2 = new BsonDouble(1.0);
        
        compareResult = comparator.compare(double1, double2);
        assertEquals(0, compareResult, "Same BSON types with same values should be equal");
    }

}
