package com.mongodb.util.bson;

import java.util.Comparator;

import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

/**
 * Implements Comparator for BsonValue, for sorting/comparing BsonValues in the same
 * way as MongoDB server.
 * 
 * @author mh
 * @see <a href="https://docs.mongodb.com/manual/reference/bson-type-comparison-order/#objects">BSON Types Comparison Order</a>
 *
 */
public class BsonValueComparator implements Comparator<BsonValue> {
    
    

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public int compare(BsonValue x, BsonValue y) {
        BsonType xType = x.getBsonType();
        BsonType yType = y.getBsonType();
        
        boolean xIsComparable = x instanceof Comparable;
        boolean yIsComparable = y instanceof Comparable;
        
        if (xType.equals(yType)) {
            
            if (xIsComparable) {
                return ((Comparable)x).compareTo((Comparable)y);
            }
            
            switch(xType) {
            case DOCUMENT:
              compareDocs((BsonDocument)x, (BsonDocument)y);
              break;
            default:
              throw new IllegalArgumentException("not implemented");
            }
            
        }
        
        
        return 0;
    }
    
    public int compareDocs(BsonDocument x, BsonDocument y) {
        String s1 = x.toJson();
        String s2 = y.toJson();
        return s1.compareTo(s2);
    }

}
