package com.mongodb.util;

import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.MaxKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Mongos;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;

public class ShardConfigSync {
    
    private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);
    
    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");
    
    private String sourceClusterUri;
    
    private String destClusterUri;
    
    private boolean dropDestinationCollectionsIfExisting;
    
    private MongoClient sourceClient;
    private MongoClient destClient;
    
    private MongoDatabase sourceConfigDb;
    private MongoDatabase destConfigDb;
    
    CodecRegistry pojoCodecRegistry;
    
    private List<Shard> sourceShards = new ArrayList<Shard>();
    private List<Shard> destShards = new ArrayList<Shard>();
    private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();
    
    private List<ShardCollection> sourceCollections = new ArrayList<ShardCollection>();
    
    
    public void run() {
        
        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        
        MongoClientURI source = new MongoClientURI(sourceClusterUri);
        sourceClient = new MongoClient(source);
        sourceClient.getDatabase("admin").runCommand(new Document("ping",1));
        sourceConfigDb = sourceClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        
        MongoClientURI dest = new MongoClientURI(destClusterUri);
        destClient = new MongoClient(dest);
        destClient.getDatabase("admin").runCommand(new Document("ping",1));
        destConfigDb = destClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        
        populateShardList(sourceConfigDb, sourceShards);
        populateShardList(destConfigDb, destShards);
        
        int index = 0;
        for (Iterator<Shard> i = sourceShards.iterator(); i.hasNext();) {
            Shard sourceShard = i.next();
            Shard destShard = destShards.get(index);
            logger.debug(sourceShard.getId() + " ==> " + destShard.getId());
            sourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
            index++;
        }
        
        stopBalancers();
        enableDestinationSharding();
        populateCollectionList(sourceConfigDb, sourceCollections);
        shardDestinationCollections();
        createDestChunks();
        if (! compareAndMoveChunks()) {
            throw new RuntimeException("chunks don't match");
        }
        
    }
    
    private void stopBalancers() {
        sourceClient.getDatabase("admin").runCommand(new Document("balancerStop",1));
        destClient.getDatabase("admin").runCommand(new Document("balancerStop",1));
    }
    
    private void createDestChunks() {
        
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));
        
        Document splitCommand = new Document();
        String lastNs = null;
        int currentCount = 0;
        
        for (Document chunk : sourceChunks) {
            String ns = chunk.getString("ns");
            
            if (! ns.equals(lastNs) && lastNs != null) {
                logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
                currentCount = 0;
            }
            
            Document max = (Document)chunk.get("max");
            boolean maxKey = false;
            for (Iterator i = max.values().iterator(); i.hasNext();) {
                Object next = i.next();
                if (next instanceof MaxKey) {
                    maxKey = true;
                    break;
                }
            }
            
            if (maxKey) {
                continue;
            }
            
            splitCommand.put("split", ns);
            splitCommand.put("middle", max);
            
            try {
                destClient.getDatabase("admin").runCommand(splitCommand);
            } catch (MongoCommandException mce) {
                logger.error(String.format("command error for namespace %s", ns), mce);
            }
            
            lastNs = ns;
            currentCount++;
        }
    }
    
    private boolean compareAndMoveChunks() {
        
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));
        
        MongoCollection<Document> destChunksColl = destConfigDb.getCollection("chunks");
        Iterator<Document> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min")).iterator();
        
        String lastNs = null;
        int currentCount = 0;
        int movedCount = 0;
        
        for (Document sourceChunk : sourceChunks) {
            
            if (!destChunks.hasNext()) {
                return false;
            }
            Document destChunk = destChunks.next();
            
            
            String sourceNs = sourceChunk.getString("ns");
            Document sourceMin = (Document)sourceChunk.get("min");
            Document sourceMax = (Document)sourceChunk.get("max");
            String sourceShard = sourceChunk.getString("shard");
            String mappedShard = sourceToDestShardMap.get(sourceShard);
            
            String destNs = destChunk.getString("ns");
            Document destMin = (Document)destChunk.get("min");
            Document destMax = (Document)destChunk.get("max");
            String destShard = destChunk.getString("shard");
            
            if (!sourceNs.equals(lastNs) && lastNs != null) {
                logger.debug(String.format("%s - chunks: %s, moved count: %s", lastNs, currentCount, movedCount));
                currentCount = 0;
                movedCount = 0;
            }
            
            if (! mappedShard.equals(destShard)) {
                logger.debug(String.format("%s: moving chunk from %s to %s", destNs, destShard, mappedShard));
                moveChunk(destNs, destMin, destMax, mappedShard);
                movedCount++;
            }
            
            if (!sourceNs.equals(destNs) && !sourceMax.equals(destMax)) {
                return false;
            }
        }
        return true;
    }
    
    private boolean compareShardCounts() {
        
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));
        
        MongoCollection<Document> destChunksColl = destConfigDb.getCollection("chunks");
        Iterator<Document> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min")).iterator();
        
        String lastNs = null;
        int currentCount = 0;
        int movedCount = 0;
        
        for (Document sourceChunk : sourceChunks) {
            
            if (!destChunks.hasNext()) {
                return false;
            }
            Document destChunk = destChunks.next();
            
            
            String sourceNs = sourceChunk.getString("ns");
            Document sourceMin = (Document)sourceChunk.get("min");
            Document sourceMax = (Document)sourceChunk.get("max");
            String sourceShard = sourceChunk.getString("shard");
            String mappedShard = sourceToDestShardMap.get(sourceShard);
            
            String destNs = destChunk.getString("ns");
            Document destMin = (Document)destChunk.get("min");
            Document destMax = (Document)destChunk.get("max");
            String destShard = destChunk.getString("shard");
            
            if (!sourceNs.equals(lastNs) && lastNs != null) {
                logger.debug(String.format("%s - chunks: %s, moved count: %s", lastNs, currentCount, movedCount));
                currentCount = 0;
                movedCount = 0;
            }
            
            if (! mappedShard.equals(destShard)) {
                logger.debug(String.format("%s: moving chunk from %s to %s", destNs, destShard, mappedShard));
                moveChunk(destNs, destMin, destMax, mappedShard);
                movedCount++;
            }
            
            if (!sourceNs.equals(destNs) && !sourceMax.equals(destMax)) {
                return false;
            }
        }
        return true;
    }
    
    private void moveChunk(String namespace, Document min, Document max, String moveToShard) {
        Document moveChunkCmd = new Document("moveChunk", namespace);
        moveChunkCmd.append("bounds", Arrays.asList(min, max));
        moveChunkCmd.append("to", moveToShard);
        destClient.getDatabase("admin").runCommand(moveChunkCmd);
    }
    
    private void populateCollectionList(MongoDatabase db, List<ShardCollection> list) {
        MongoCollection<ShardCollection> shardsColl = db.getCollection("collections", ShardCollection.class);
        shardsColl.find(eq("dropped", false)).sort(Sorts.ascending("_id")).into(list);
    }
    
    private void populateMongosList(MongoDatabase db, List<Mongos> list) {
        MongoCollection<Mongos> mongosColl = db.getCollection("mongos", Mongos.class);
        
        LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);
        
        mongosColl.find(eq("ping", false)).sort(Sorts.ascending("pint")).into(list);
    }
    
    private void shardDestinationCollections() {
        for (ShardCollection sourceColl : sourceCollections) {
            Document shardCommand = new Document("shardCollection", sourceColl.getId());
            shardCommand.append("key", sourceColl.getKey());
            shardCommand.append("unique", sourceColl.isUnique());
            if (sourceColl.getDefaultCollation() != null) {
                shardCommand.append("collation", LOCALE_SIMPLE);
            }
            try {
                destClient.getDatabase("admin").runCommand(shardCommand);
            } catch (MongoCommandException mce) {
                if (dropDestinationCollectionsIfExisting && mce.getCode() == 20) {
                    logger.debug(String.format("Sharding already enabled for %s", sourceColl.getId()));
                } else {
                    throw mce;
                }
            }
            
            if (sourceColl.isNoBalance()) {
                // TODO there is no disableBalancing command so this is not possible in Atlas
                //destClient.getDatabase("admin").runCommand(new Document("", ""));
                logger.warn(String.format("Balancing is disabled for %s, this is not possible in Atlas", sourceColl.getId()));
            }
            
        }
    }
    
    private void populateShardList(MongoDatabase db, List<Shard> list) {
        MongoCollection<Shard> shardsColl = db.getCollection("shards", Shard.class);
        shardsColl.find().sort(Sorts.ascending("_id")).into(list);
    }
    
    
    public void enableDestinationSharding() {
        MongoCollection<Document> databasesColl = sourceConfigDb.getCollection("databases");
        FindIterable<Document> databases = databasesColl.find(eq("partitioned", true));
        for (Document database : databases) {
            String databaseName = database.getString("_id");
            try {
                destClient.getDatabase("admin").runCommand(new Document("enableSharding", databaseName));
            } catch (MongoCommandException mce) {
                if (dropDestinationCollectionsIfExisting && mce.getCode() == 23) {
                    logger.debug(String.format("Destination database %s exists, dropping", databaseName));
                    destClient.dropDatabase(databaseName);
                    destClient.getDatabase("admin").runCommand(new Document("enableSharding", databaseName));
                } else {
                    throw mce;
                }
            }
        }
    }

    public String getSourceClusterUri() {
        return sourceClusterUri;
    }

    public void setSourceClusterUri(String sourceClusterUri) {
        this.sourceClusterUri = sourceClusterUri;
    }

    public String getDestClusterUri() {
        return destClusterUri;
    }

    public void setDestClusterUri(String destClusterUri) {
        this.destClusterUri = destClusterUri;
    }

    public boolean isDropDestinationCollectionsIfExisting() {
        return dropDestinationCollectionsIfExisting;
    }

    public void setDropDestinationCollectionsIfExisting(boolean dropDestinationCollectionsIfExisting) {
        this.dropDestinationCollectionsIfExisting = dropDestinationCollectionsIfExisting;
    }
}
