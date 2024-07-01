package com.mongodb.util.bson;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.junit.jupiter.api.Test;

import com.mongodb.shardbalancer.CountingMegachunk;

public class BsonValueWrapperTest {
	
	private final static BsonMinKey min = new BsonMinKey();
    private final static BsonMaxKey max = new BsonMaxKey();
    
    
   
    public void testCompareDocuments() {
    	
    	 BsonDocument d0 = new BsonDocument()
                 .append("x", min)
                 .append("o", min)
                 .append("i", min);
    	
        BsonDocument d1 = new BsonDocument()
                .append("x", new BsonString("AUS"))
                .append("o", min)
                .append("i", min);
                		
        BsonDocument d2 = new BsonDocument()
                .append("x", new BsonString("AUS"))
                .append("o", new BsonInt64(902197565050L))
                .append("i", new BsonInt64(123035482451070L));
        
        BsonDocument d3 = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", min)
                .append("i", min);
        
        BsonDocument d4a = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", min)
                .append("i", new BsonInt64(122989409627060L));
        
        BsonDocument d4 = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", new BsonInt64(102197565050L))
                .append("i", new BsonDouble(522989409627060L));
        
        BsonDocument d4b = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", new BsonInt64(102197565050L))
                .append("i", new BsonInt64(522989409627061L));
        
        BsonDocument d5 = new BsonDocument()
                .append("x", new BsonString("AUS"))
                .append("o", new BsonInt64(86617050L))
                .append("i", new BsonInt64(2754265070L));
        
        
        BsonDocument d6 = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", new BsonInt64(35270050L))
                .append("i", new BsonInt64(2768454060L));
        
        BsonDocument d7 = new BsonDocument()
                .append("x", new BsonString("SFO"))
                .append("o", max)
                .append("i", min);
        
        NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = new TreeMap<>();
        innerMap.put(new BsonValueWrapper(d7), null);
        innerMap.put(new BsonValueWrapper(d6), null);
        innerMap.put(new BsonValueWrapper(d5), null);
        innerMap.put(new BsonValueWrapper(d4), null);
        innerMap.put(new BsonValueWrapper(d3), null);
        innerMap.put(new BsonValueWrapper(d2), null);
        innerMap.put(new BsonValueWrapper(d1), null);
        innerMap.put(new BsonValueWrapper(d0), null);
        innerMap.put(new BsonValueWrapper(d4a), null);
        innerMap.put(new BsonValueWrapper(d4b), null);
             
        
        for (BsonValueWrapper w : innerMap.keySet()) {
        	System.out.println(w);
        }
    }
    
    @Test
    public void testCompareDocuments2() {
    	
   	 	BsonDocument d1 = new BsonDocument()
                .append("x", min)
                .append("o", min);
   	
               		
       BsonDocument d2 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", min);
       
       BsonDocument d3 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", new BsonDouble(-1.0E16))
           ;
       
       BsonDocument d4 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", new BsonDouble(-2.142225352E9));
       
       BsonDocument d5 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", new BsonInt32(-2123597549));
       
       BsonDocument d6 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", new BsonInt32(134994694));
       
       BsonDocument d7 = new BsonDocument()
               .append("x", new BsonString("AUS"))
               .append("o", new BsonInt32(412761391));
       
       
       
       NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = new TreeMap<>();
       innerMap.put(new BsonValueWrapper(d7), null);
       innerMap.put(new BsonValueWrapper(d6), null);
       innerMap.put(new BsonValueWrapper(d5), null);
       innerMap.put(new BsonValueWrapper(d4), null);
       innerMap.put(new BsonValueWrapper(d3), null);
       innerMap.put(new BsonValueWrapper(d2), null);
       innerMap.put(new BsonValueWrapper(d1), null);
       
       
       BsonDocument d = new BsonDocument().append("dataCenter", new BsonString("AUS")).append("accountHash", new BsonInt32(139734917));
		BsonValueWrapper id = new BsonValueWrapper(d);
		Map.Entry<BsonValueWrapper, CountingMegachunk> entry = innerMap.floorEntry(id);
            
       System.out.println(entry);
       for (BsonValueWrapper w : innerMap.keySet()) {
       	System.out.println(w);
       }
   }

}
