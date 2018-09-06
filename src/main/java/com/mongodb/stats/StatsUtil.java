package com.mongodb.stats;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class StatsUtil {
    
    private String mongoUri;
    private String database;
    private String collection;
    private String groupField;
    
    DescriptiveStatistics sizeStats = new DescriptiveStatistics();
    private Map<Object, DescriptiveStatistics> statsMap = new TreeMap<Object, DescriptiveStatistics>();
    
    private MongoClient client;

    public void setMongoUri(String mongoUri) {
        this.mongoUri = mongoUri;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setCollection(String collection) {
        this.collection = collection;
    }

    public void setGroupField(String groupField) {
        this.groupField = groupField;
    }

    public void init() {
        MongoClientURI source = new MongoClientURI(mongoUri);
        client = new MongoClient(source);
    }
    
    public void stats() {
        MongoDatabase db = client.getDatabase(database);
        MongoCollection<RawBsonDocument> mongoCollection = db.getCollection(collection, RawBsonDocument.class);
        FindIterable<RawBsonDocument> findIterable = mongoCollection.find();
        findIterable.noCursorTimeout(true);
        MongoCursor<RawBsonDocument> cursor = findIterable.iterator();
        while (cursor.hasNext()) {
            RawBsonDocument doc = cursor.next();
            int size = doc.getByteBuffer().remaining();
            sizeStats.addValue(size);
            
            BsonValue groupKey = getNested(groupField, doc);
            DescriptiveStatistics value = statsMap.get(groupKey);
            if (value == null) {
                value = new DescriptiveStatistics();
                value.addValue(size);
                statsMap.put(groupKey, value);
            }
        }
        
        for (Map.Entry<Object, DescriptiveStatistics> entry : statsMap.entrySet()) {
            printStats(entry.getKey().toString(), entry.getValue());
        }
        
        
    }
    
    private static BsonValue getNested(String key, BsonDocument doc) {
        String[] keys = key.split("\\.");
        BsonValue current = null;
        for (String k : keys) {
            if (current != null) {
                current = ((BsonDocument)current).get(k);
            } else {
                current = doc.get(k);
            }
        }
        return current;
    }
    
    private static void printStats(String key, DescriptiveStatistics sizeStats) {
        double avg = sizeStats.getMean();
        double max = sizeStats.getMax();
        double p95 = sizeStats.getPercentile(95);
        double total = sizeStats.getSum();
        
        System.out.println(String.format("%20s: Avg: %6.0f, max: %6.0f, 95p: %6.0f, total: %6.0f", key, avg, max, p95, total));
    }

}
