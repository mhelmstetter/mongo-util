package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.MaxKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Mongos;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;

public class ShardConfigSync {
    
    private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);
    
    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";
    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");
    
    private String sourceClusterUri;
    
    private String destClusterUri;
    private String destUsername;
    private String destPassword;
    
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
    
    private List<Mongos> destMongos = new ArrayList<Mongos>();
    private List<MongoClient> destMongoClients = new ArrayList<MongoClient>();
    
    private ExecutorService executor = Executors.newFixedThreadPool(32);
    
    public void run() throws InterruptedException {
        
        logger.debug("ShardConfigSync starting");
        
        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
        
        MongoClientURI source = new MongoClientURI(sourceClusterUri);
        
        sourceClient = new MongoClient(source);
        List<ServerAddress> addrs = sourceClient.getServerAddressList();
        sourceClient.getDatabase("admin").runCommand(new Document("ping",1));
        sourceConfigDb = sourceClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        
        
        boolean isSRVProtocol = destClusterUri.startsWith(MONGODB_SRV_PREFIX);
        if (isSRVProtocol) {
            throw new RuntimeException("mongodb+srv protocol not supported use standard mongodb protocol in connection string");
        }
        
        MongoClientURI dest = new MongoClientURI(destClusterUri);
        this.destUsername = dest.getUsername();
        this.destPassword = String.valueOf(dest.getPassword());
        
        
        // We need to ensure a consistent connection to only a single mongos
        // assume that we will only use the first one in the list
        if (dest.getHosts().size() > 1) {
            throw new RuntimeException("Specify only a single destination mongos in the connection string");
        }
        
        destClient = new MongoClient(dest);
        
        destClient.getDatabase("admin").runCommand(new Document("ping",1));
        destConfigDb = destClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        
        
        populateShardList(sourceConfigDb, sourceShards);
        populateShardList(destConfigDb, destShards);
        
        populateMongosList(destConfigDb, destMongos);
        
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
        
        //Thread.sleep(1000);
        //flushRouterConfig();
        
        populateCollectionList(sourceConfigDb, sourceCollections);
        shardDestinationCollections();
        
        // try to workaround errors
        //{ "code" : 118, "ok" : 0.0, "errmsg" : "Collection d1.foo3 is not sharded." }
        //Thread.sleep(10000);
        //flushRouterConfig();
       
        createDestChunks();
        
        if (! compareAndMoveChunks()) {
            throw new RuntimeException("chunks don't match");
        }
        
        flushRouterConfig();
        
    }
    
    private void stopBalancers() {
        logger.debug("stopBalancers started");
        sourceClient.getDatabase("admin").runCommand(new Document("balancerStop",1));
        destClient.getDatabase("admin").runCommand(new Document("balancerStop",1));
        logger.debug("stopBalancers complete");
    }
    
    private void createDestChunks() {
        logger.debug("createDestChunks started");
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
        logger.debug("createDestChunks complete");
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
                logger.error("No destination chunks found");
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
            
            
            String destNs = destChunk.getString("ns");
            Document destMin = (Document)destChunk.get("min");
            Document destMax = (Document)destChunk.get("max");
            
            
            
        }
        return true;
    }
    
    // TODO - this needs to flush on all routers?
    private void flushRouterConfig() {
        logger.debug("flushRouterConfig()");
        for (MongoClient client : destMongoClients) {
            Document flushRouterConfig = new Document("flushRouterConfig", true);
            logger.debug(String.format("flushRouterConfig for mongos %s", client.getAddress()));
            client.getDatabase("admin").runCommand(flushRouterConfig);
        }
        
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
        //LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);
        mongosColl.find(eq("ping", false)).sort(Sorts.ascending("ping")).into(list);
        for (Mongos mongos : destMongos) {
            String uri = "mongodb://" + destUsername + ":" + destPassword + "@" + mongos.getId(); 
            MongoClientURI clientUri = new MongoClientURI(uri);
            MongoClient client = new MongoClient(clientUri);
            destMongoClients.add(client);
        }
    }
    
    private void shardDestinationCollections() {
        logger.debug("shardDestinationCollections() started");
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
        logger.debug("shardDestinationCollections() complete");
    }
    
    private void populateShardList(MongoDatabase db, List<Shard> list) {
        MongoCollection<Shard> shardsColl = db.getCollection("shards", Shard.class);
        shardsColl.find().sort(Sorts.ascending("_id")).into(list);
    }
    
    
    public void enableDestinationSharding() {
        logger.debug("enableDestinationSharding()");
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
        logger.debug("enableDestinationSharding() complete");
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
