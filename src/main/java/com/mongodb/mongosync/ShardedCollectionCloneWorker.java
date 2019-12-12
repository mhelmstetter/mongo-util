package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;

public class ShardedCollectionCloneWorker extends AbstractCollectionCloneWorker implements Runnable {

    public ShardedCollectionCloneWorker(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient,
            MongoSyncOptions options) {
        super(ns, sourceShardClient, destShardClient, options);
        // TODO Auto-generated constructor stub
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub
        
    }
    
//    protected static final Logger logger = LoggerFactory.getLogger(ShardedCollectionCloneWorker.class);
//    
//    public ShardedCollectionCloneWorker(Namespace ns, ShardClient sourceShardClient, ShardClient destShardClient, MongoSyncOptions options) {
//        super(ns, sourceShardClient, destShardClient, options);
//    }
//
//    @Override
//    public void run() {
//        
//        CodecRegistry codecRegistry = MongoClient.getDefaultCodecRegistry();
//        
//        MongoCursor<RawBsonDocument> cursor = null;
//        long start = System.currentTimeMillis();
//        long last = start;
//        long count = 0;
//        Document shardKeysDoc = (Document)shardCollection.get("key");
//        Set<String> shardKeys = shardKeysDoc.keySet();
//        Number total = ShardClient.getFastCollectionCount(sourceDb, sourceCollection);
//        
//        MongoCollection<Document> sourceChunksColl = sourceShardClient.getChunksCollection();
//        FindIterable<Document> sourceChunks = sourceChunksColl.find(eq("ns", this.ns.getNamespace())).sort(Sorts.ascending("min"));
//        for (Document sourceChunk : sourceChunks) {
//            String id  = sourceChunk.getString("_id");
//            // each chunk is inclusive of min and exclusive of max
//            Document min = (Document)sourceChunk.get("min");
//            Document max = (Document)sourceChunk.get("max");
//            Bson chunkQuery = null;
//            
//            if (shardKeys.size() > 1) {
//                List<Bson> filters = new ArrayList<Bson>(shardKeys.size());
//                for (String key : shardKeys) {
//                    filters.add(and(gte(key, min.get(key)), lt(key, max.get(key))));
//                }
//                chunkQuery = and(filters);
//            } else {
//                String key = shardKeys.iterator().next();
//                chunkQuery = and(gte(key, min.get(key)), lt(key, max.get(key)));
//            }
//            
//            try {
//                cursor = sourceCollection.find(chunkQuery).noCursorTimeout(true).hint(new Document("_id", 1)).iterator();
//                //
//                while (cursor.hasNext()) {
//                    RawBsonDocument doc = cursor.next();
//                    count++;
//                    
////                    BsonValue id = doc.get("_id");
////                    BsonType idType = id.getBsonType();
////                    if (idType.equals(BsonType.DOCUMENT)) {
////                        byte[] idBytes = id.asBinary().getData();
////                        
////                    }
////                    
////                    byte[] sourceBytes = doc.getByteBuffer().array();
////                    byte[] sourceHash = CodecUtils.md5(sourceBytes);
////                    Document hashDoc = new Document("_id", doc.get("_id"));
////                    hashDoc.put("len", sourceBytes.length);
////                    hashDoc.put("md5", sourceHash);
//                    buffer.add(new InsertOneModel<RawBsonDocument>(doc));
//                    if (buffer.size() >= options.getBatchSize()) {
//                        doInsert();
//                        buffer.clear();
//                        
//                        long current = System.currentTimeMillis();
//                        long delta = (current - last) / 1000;
//                        if (delta >= 30) {
//                            logger.debug(String.format("Cloned %s / %s documents", count, total));
//                            last = current;
//                        }
//                    }
//                    
//                }
//                // flush any remaining from the buffer
//                if (buffer.size() > 0) {
//                    doInsert();
//                    buffer.clear();
//                }
//                
//            } finally {
//                cursor.close();
//            }
//        }
//        long end = System.currentTimeMillis();
//        Double dur = (end - start)/1000.0;
//        logger.debug(String.format("\nDone cloning %s, %s documents in %f seconds", ns, count, dur));
//        
//    }

}
