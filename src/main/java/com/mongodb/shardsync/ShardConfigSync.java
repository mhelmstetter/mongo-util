package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Filters.exists;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.exec.ExecuteException;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.UuidCodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.types.MaxKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;
import com.mongodb.mongomirror.MongoMirrorRunner;

// TODO - look at https://github.com/10gen/scripts-and-snippets/blob/master/mongod/recreate-splits.js
public class ShardConfigSync {

    private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);
    
    private final static int BATCH_SIZE = 512;

    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");

    private String sourceClusterUri;

    private String destClusterUri;
    

    private boolean dropDestDbs;
    private boolean dropDestDbsAndConfigMetadata;
    private boolean nonPrivilegedMode = false;
    private boolean doChunkCounts;
    private boolean preserveUUIDs;
    private boolean tailOnly;
    
    private ShardClient sourceShard;
    private ShardClient destShard;

    private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();

    
    private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
    private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();
    
    private boolean filtered = false;
    private String[] namespaceFilterList;
    private Set<Namespace> namespaceFilters = new HashSet<Namespace>();
    private Set<String> databaseFilters = new HashSet<String>();
    
    private String[] shardMap;
    
    private File mongomirrorBinary;

    private long sleepMillis;
    
    private String  numParallelCollections;
    
    
    private String destVersion;
    private List<Integer> destVersionArray;
    
    private boolean sslAllowInvalidHostnames;
    private boolean sslAllowInvalidCertificates;
    
    CodecRegistry registry = fromRegistries(fromProviders(new UuidCodecProvider(UuidRepresentation.STANDARD)),
            MongoClient.getDefaultCodecRegistry());
    DocumentCodec documentCodec = new DocumentCodec(registry);

    public ShardConfigSync() {
        logger.debug("ShardConfigSync starting");
    }

    public void init() {
        logger.debug("Start init()");
        sourceShard = new ShardClient("source", sourceClusterUri);
        destShard = new ShardClient("dest", destClusterUri);
        initializeShardMappings();
    }
    
    
    private void initializeShardMappings() {
        if (this.shardMap != null) {
            // shardMap is for doing an uneven shard mapping, e.g. 10 shards on source
            // down to 5 shards on destination
            logger.debug("Custom n:m shard mapping");
            for (String mapping : shardMap) {
                String[] mappings = mapping.split("\\|");
                logger.debug(mappings[0] + " ==> " + mappings[1]);
                sourceToDestShardMap.put(mappings[0], mappings[1]);
            }
        } else {
            logger.debug("Default 1:1 shard mapping");
            logger.debug("Source shard count: " + sourceShard.getShardsMap().size());
            // default, just match up the shards 1:1
            int index = 0;
            List<Shard> destList = new ArrayList<Shard>(destShard.getShardsMap().values());
            for (Iterator<Shard> i = sourceShard.getShardsMap().values().iterator(); i.hasNext();) {
                Shard sourceShard = i.next();
                Shard destShard = destList.get(index);
                if (destShard != null) {
                    logger.debug(sourceShard.getId() + " ==> " + destShard.getId());
                    sourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
                }
                index++;
            }
        }
    }
    
    public void doSharding() {
       
        logger.debug("Starting metadata sync/migration");

        //stopBalancers();
        // TODO disableAutoSplit !!!!
        //enableDestinationSharding();

        sourceShard.populateCollectionsMap();
        shardDestinationCollections();
    }

    public void migrateMetadata() throws InterruptedException {
        logger.debug("Starting metadata sync/migration");

        stopBalancers();
        //checkAutosplit();
        enableDestinationSharding();

        sourceShard.populateCollectionsMap();
        shardDestinationCollections();
        
        if (nonPrivilegedMode) {
            createDestChunksUsingSplitCommand();
        } else {
            createDestChunksUsingInsert();
            createShardTagsUsingInsert();
        }

        if (!compareAndMoveChunks(true)) {
            throw new RuntimeException("chunks don't match");
        }

        destShard.flushRouterConfig();
    }

    private void stopBalancers() {
        
        logger.debug("stopBalancers started");
        sourceShard.stopBalancer();
        destShard.stopBalancer();
        logger.debug("stopBalancers complete");
    }
    
    private void checkAutosplit() {
         sourceShard.checkAutosplit();
    }

    /**
     * Create chunks on the dest side using the "split" runCommand
     * NOTE that this will not work correctly with hashed shard keys
     * NOTE that this will be very slow b/c of the locking process that happens with each chunk
     * In these cases where the dest is Atls, we need to get elevated permissions in Atlas
     * so that we can bulk insert the dest chunks.
     */
    private void createDestChunksUsingSplitCommand() {
        logger.debug("createDestChunksUsingSplitCommand started");
        MongoCollection<Document> sourceChunksColl = sourceShard.getChunksCollection();
        MongoCollection<Document> destChunksColl = destShard.getChunksCollection();
        FindIterable<Document> sourceChunks = sourceChunksColl.find().noCursorTimeout(true).sort(Sorts.ascending("ns", "max"));

        Document splitCommand = new Document();
        String lastNs = null;
        int currentCount = 0;

        for (Iterator<Document> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {
            
            Document chunk = sourceChunksIterator.next();
            String ns = chunk.getString("ns");
            Namespace sourceNs = new Namespace(ns);
            
            if (filtered && ! namespaceFilters.contains(sourceNs) && !databaseFilters.contains(sourceNs.getDatabaseName())) {
                continue;
            }
            
            //TODO make this configurable
            if (! dropDestDbs) {
                long count = destChunksColl.countDocuments(new Document("_id", chunk.get("_id")));
                if (count > 0) {
                    continue;
                }
            }

            if (!ns.equals(lastNs) && lastNs != null) {
                logger.debug(String.format("%s - created %s chunks", lastNs, ++currentCount));
                currentCount = 0;
            }

            Document max = (Document) chunk.get("max");
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
                destShard.adminCommand(splitCommand);
            } catch (MongoCommandException mce) {
                logger.error(String.format("command error for namespace %s", ns), mce);
            }

            lastNs = ns;
            currentCount++;
        }
        logger.debug("createDestChunksUsingSplitCommand complete");
    }
    
    @SuppressWarnings("unchecked")
    private void createShardTagsUsingInsert() {
        logger.debug("createShardTagsUsingInsert started");
        
        MongoCollection<Document> sourceShardsColl = sourceShard.getShardsCollection();
        FindIterable<Document> sourceShards = sourceShardsColl.find(exists("tags.0"));
        for (Iterator<Document> it = sourceShards.iterator(); it.hasNext();) {
            Document shard = it.next();
            String sourceShardName = shard.getString("_id");
            String mappedShard = sourceToDestShardMap.get(sourceShardName);
            List<String> tags = (List<String>)shard.get("tags");
            
            for (String tag : tags) {
                Document command = new Document("addShardToZone", mappedShard).append("zone", tag);
                logger.debug(String.format("addShardToZone('%s', '%s')", mappedShard, tag));
                destShard.adminCommand(command);
            }
        }
        
        MongoCollection<Document> sourceTagsColl = sourceShard.getTagsCollection();
        FindIterable<Document> sourceTags = sourceTagsColl.find().sort(Sorts.ascending("ns", "min"));
        
        for (Iterator<Document> it = sourceTags.iterator(); it.hasNext();) {
            
            Document tag = it.next();
            logger.trace("tag: " + tag);
            String ns = tag.getString("ns");
            Namespace sourceNs = new Namespace(ns);
            if (filtered && ! namespaceFilters.contains(sourceNs) && !databaseFilters.contains(sourceNs.getDatabaseName())) {
                continue;
            }
            
            Document command = new Document("updateZoneKeyRange", ns);
            command.append("min", tag.get("min"));
            command.append("max", tag.get("max"));
            command.append("zone", tag.get("tag"));
            destShard.adminCommand(command);
        }
        logger.debug("createShardTagsUsingInsert complete");
    }
    
    /**
     * Alternative to createDestChunksUsingSplitCommand(). Preferred approach for
     * simplicity and performance, but this requires special permissions in Atlas.
     */
    private void createDestChunksUsingInsert() {
        logger.debug("createDestChunksUsingInsert started");
        MongoCollection<Document> sourceChunksColl = sourceShard.getChunksCollection();
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));

        String lastNs = null;
        int currentCount = 0;

        for (Iterator<Document> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {
            
            Document chunk = sourceChunksIterator.next();
            String ns = chunk.getString("ns");
            Namespace sourceNs = new Namespace(ns);
            if (filtered && ! namespaceFilters.contains(sourceNs) && !databaseFilters.contains(sourceNs.getDatabaseName())) {
                continue;
            }
            if (sourceNs.getDatabaseName().equals("config")) {
                continue;
            }
            
            String sourceShardName = chunk.getString("shard");
            String mappedShard = sourceToDestShardMap.get(sourceShardName);
            chunk.append("shard", mappedShard);
            
            if (!ns.equals(lastNs) && lastNs != null) {
                logger.debug(String.format("%s - created %s chunks", lastNs, ++currentCount));
                currentCount = 0;
            }

            try {
                
                // hack to avoid "Invalid BSON field name _id.x" for compound shard keys
                RawBsonDocument rawDoc = new RawBsonDocument(chunk, documentCodec);
                destShard.getChunksCollectionRaw().insertOne(rawDoc);
                
            } catch (MongoException mce) {
                logger.error(String.format("command error for namespace %s", ns), mce);
            }

            lastNs = ns;
            currentCount++;
        }
        
        logger.debug("createDestChunksUsingInsert complete");
    }

    public void compareChunks() {
        compareAndMoveChunks(false);
    }
    
    public void diffChunks(String dbName) {
        
        Map<String, Document> sourceChunkMap = new HashMap<String, Document>();
        MongoCollection<Document> sourceChunksColl = sourceShard.getChunksCollection();
        FindIterable<Document> sourceChunks = sourceChunksColl.find(regex("ns", "^" + dbName + "\\.")).sort(Sorts.ascending("ns", "min"));
        for (Document sourceChunk : sourceChunks) {
            String id  = sourceChunk.getString("_id");
            sourceChunkMap.put(id, sourceChunk);
        }
        logger.debug("Done reading source chunks, count = " + sourceChunkMap.size());
        
        
        
        logger.debug("Reading destination chunks");
        Map<String, Document> destChunkMap = new HashMap<String, Document>();
        MongoCollection<Document> destChunksColl = destShard.getChunksCollection();
        FindIterable<Document> destChunks = destChunksColl.find(regex("ns", "^" + dbName + "\\.")).sort(Sorts.ascending("ns", "min"));
       
        for (Document destChunk : destChunks) {
            String id  = destChunk.getString("_id");
            destChunkMap.put(id, destChunk);
            
            Document sourceChunk = sourceChunkMap.get(id);
            if (sourceChunk == null) {
                logger.debug("Source chunk not found: " + id);
                continue;
            }
            String sourceShard = sourceChunk.getString("shard");
            String mappedShard = sourceToDestShardMap.get(sourceShard);
            if (mappedShard == null) {
                throw new IllegalArgumentException("No destination shard mapping found for source shard: " + sourceShard);
            }
            
            String destShard = destChunk.getString("shard");
            if (! destShard.equals(mappedShard)) {
                logger.warn("Chunk on wrong shard: " + id);
            }
            
            
        }
        logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
        
    }

    private boolean compareAndMoveChunks(boolean doMove) {
        
        logger.debug("Reading destination chunks, doMove: " + doMove);
        Map<String, String> destChunkMap = new HashMap<String, String>();
        MongoCollection<Document> destChunksColl = destShard.getChunksCollection();
        FindIterable<Document> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min"));
       
        for (Document destChunk : destChunks) {
            String id  = destChunk.getString("_id");
            String shard = destChunk.getString("shard");
            destChunkMap.put(id, shard);
        }
        logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
        
        
        MongoCollection<Document> sourceChunksColl = sourceShard.getChunksCollection();
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));

        String lastNs = null;
        int currentCount = 0;
        int movedCount = 0;
        int mismatchedCount = 0;
        int matchedCount = 0;

        for (Document sourceChunk : sourceChunks) {

            String sourceNs = sourceChunk.getString("ns");
            Namespace sourceNamespace = new Namespace(sourceNs);
            
            if (filtered && ! namespaceFilters.contains(sourceNamespace) && !databaseFilters.contains(sourceNamespace.getDatabaseName())) {
                continue;
            }
            
            Document sourceMin = (Document) sourceChunk.get("min");
            Document sourceMax = (Document) sourceChunk.get("max");
            String sourceShard = sourceChunk.getString("shard");
            String mappedShard = sourceToDestShardMap.get(sourceShard);
            if (mappedShard == null) {
                throw new IllegalArgumentException("No destination shard mapping found for source shard: " + sourceShard);
            }
            String sourceId = sourceChunk.getString("_id");
            
            String destShard = destChunkMap.get(sourceId);
            
            if (destShard == null) {
                logger.error("Chunk with _id " + sourceId + " not found on destination");
                continue;
            }
            

            if (doMove && !mappedShard.equals(destShard)) {
                logger.debug(String.format("%s: moving chunk from %s to %s", sourceNs, destShard, mappedShard));
                if (doMove) {
                    moveChunk(sourceNs, sourceMin, sourceMax, mappedShard);
                    continue;
                }

                movedCount++;
            } else if (!doMove) {
                if (!mappedShard.equals(destShard)) {
                    logger.debug("dest chunk is on wrong shard for sourceChunk: " + sourceChunk);
                    mismatchedCount++;
                }
                matchedCount++;
            }

            lastNs = sourceNs;
        }
        logger.debug("Matched count: " + matchedCount);
        return true;
    }

    @SuppressWarnings("unchecked")
    public void compareShardCounts() {

        logger.debug("Starting compareShardCounts mode");

        Document listDatabases = new Document("listDatabases", 1);
        Document sourceDatabases = sourceShard.adminCommand(listDatabases);
        Document destDatabases = destShard.adminCommand(listDatabases);

        List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
        populateDbMap(destDatabaseInfo, destDbInfoMap);

        for (Document sourceInfo : sourceDatabaseInfo) {
            String dbName = sourceInfo.getString("name");
            
            if (filtered && !databaseFilters.contains(dbName) || dbName.equals("config")) {
                logger.debug("Ignore " + dbName + " for compare, filtered");
                continue;
            }
                
            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo != null) {
                logger.debug(String.format("Found matching database %s", dbName));

                MongoDatabase sourceDb = sourceShard.getMongoClient().getDatabase(dbName);
                MongoDatabase destDb = destShard.getMongoClient().getDatabase(dbName);
                MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
                for (String collectionName : sourceCollectionNames) {
                    if (collectionName.startsWith("system.")) {
                        continue;
                    }
                    
                    boolean firstTry = doCounts(sourceDb, destDb, collectionName);
                    
                    if (! firstTry) {
                        doCounts(sourceDb, destDb, collectionName);
                    }
                }
            } else {
                logger.warn(String.format("Destination db not found, name: %s", dbName));
            }
        }
    }
    

    
    
    
    private boolean doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
      
        Number sourceCount = ShardClient.getCollectionCount(sourceDb, collectionName);
        Number destCount = ShardClient.getCollectionCount(destDb, collectionName);;
        
        if (sourceCount == null && destCount == null) {
            logger.debug(String.format("%s.%s count matches: %s", sourceDb.getName(), collectionName, 0));
            return true;
        } else if (sourceCount != null && sourceCount.equals(destCount)) {
            logger.debug(String.format("%s.%s count matches: %s", sourceDb.getName(), collectionName, sourceCount));
            return true;
        } else {
            logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s", sourceDb.getName(), collectionName,
                    sourceCount, destCount));
            return false;
        }
    }

    // TODO - this is incomplete
    private void compareChunkCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
        String ns = sourceDb.getName() + "." + collectionName;
        MongoCollection<Document> sourceChunksColl = sourceShard.getChunksCollection();
        FindIterable<Document> sourceChunks = sourceChunksColl.find(eq("ns", ns)).sort(Sorts.ascending("ns", "min"));

        MongoCollection<Document> destChunksColl = destShard.getChunksCollection();
        Iterator<Document> destChunks = destChunksColl.find(eq("ns", ns)).sort(Sorts.ascending("ns", "min")).iterator();

    }

    private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
        for (Document dbInfo : dbInfoList) {
            databaseMap.put(dbInfo.getString("name"), dbInfo);
        }
    }

    private void moveChunk(String namespace, Document min, Document max, String moveToShard) {
        Document moveChunkCmd = new Document("moveChunk", namespace);
        moveChunkCmd.append("bounds", Arrays.asList(min, max));
        moveChunkCmd.append("to", moveToShard);
        try {
            destShard.adminCommand(moveChunkCmd);
        } catch (MongoCommandException mce) {
            logger.warn("moveChunk error", mce);
        }
        
    }
    
    public void shardDestinationCollections() {
        if (nonPrivilegedMode) {
            logger.debug("shardDestinationCollections(), non-privileged mode");
            shardDestinationCollectionsUsingShardCommand();
        } else {
            logger.debug("shardDestinationCollections(), privileged mode");
            shardDestinationCollectionsUsingInsert();
        }
    }
    
    private void shardDestinationCollectionsUsingInsert() {
        logger.debug("shardDestinationCollectionsUsingInsert(), privileged mode");
        
        MongoCollection<RawBsonDocument> destColls = destShard.getConfigDb().getCollection("collections", RawBsonDocument.class);
        ReplaceOptions options = new ReplaceOptions().upsert(true);
        
        
        
        for (Document sourceColl : sourceShard.getCollectionsMap().values()) {
            
            String nsStr = (String)sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);
            
            if (filtered && ! namespaceFilters.contains(ns) && !databaseFilters.contains(ns.getDatabaseName())) {
                logger.debug("Namespace " + ns + " filtered, not sharding on destination");
                continue;
            }
            if (ns.getDatabaseName().equals("config")) {
                continue;
            }
            
            // hack to avoid "Invalid BSON field name _id.x" for compound shard keys
            RawBsonDocument rawDoc = new RawBsonDocument(sourceColl, documentCodec);
            destColls.replaceOne(new Document("_id", nsStr), rawDoc, options);
        }
        
        logger.debug("shardDestinationCollectionsUsingInsert() complete");
    }

    private void shardDestinationCollectionsUsingShardCommand() {
        logger.debug("shardDestinationCollectionsUsingShardCommand(), non-privileged mode");
        
        for (Document sourceColl : sourceShard.getCollectionsMap().values()) {
            
            String nsStr = (String)sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);
            
            if (filtered && ! namespaceFilters.contains(ns) && !databaseFilters.contains(ns.getDatabaseName())) {
                logger.debug("Namespace " + ns + " filtered, not sharding on destination");
                continue;
            }
            if (ns.getDatabaseName().equals("config")) {
                continue;
            }
            shardCollection(sourceColl);

            if ((boolean)sourceColl.get("noBalance")) {
                // TODO there is no disableBalancing command so this is not
                // possible in Atlas
                // destClient.getDatabase("admin").runCommand(new Document("",
                // ""));
                logger.warn(String.format("Balancing is disabled for %s, this is not possible in Atlas",
                        nsStr));
            }
        }
        logger.debug("shardDestinationCollectionsUsingShardCommand() complete");
    }
    
    /**
     * Take the sourceColl as a "template" to shard on the destination side
     * @param sourceColl
     */
    private Document shardCollection(ShardCollection sourceColl) {
        Document shardCommand = new Document("shardCollection", sourceColl.getId());
        shardCommand.append("key", sourceColl.getKey());
        
        // apparently unique is not always correct here, there are cases where unique is false
        // here but the underlying index is unique
        shardCommand.append("unique", sourceColl.isUnique());
        if (sourceColl.getDefaultCollation() != null) {
            shardCommand.append("collation", LOCALE_SIMPLE);
        }
        
        Document result = null;
        try {
            result = destShard.adminCommand(shardCommand);
        } catch (MongoCommandException mce) {
            if (mce.getCode() == 20) {
                logger.debug(String.format("Sharding already enabled for %s", sourceColl.getId()));
            } else {
                throw mce;
            }
        }
        return result;
    }
    
    private Document shardCollection(Document sourceColl) {
        Document shardCommand = new Document("shardCollection", sourceColl.get("_id"));
        shardCommand.append("key", sourceColl.get("key"));
        
        // apparently unique is not always correct here, there are cases where unique is false
        // here but the underlying index is unique
        shardCommand.append("unique", sourceColl.get("unique"));
        
        // TODO fixme!!!
//        if (sourceColl.getDefaultCollation() != null) {
//            shardCommand.append("collation", LOCALE_SIMPLE);
//        }
        
        Document result = null;
        try {
            result = destShard.adminCommand(shardCommand);
        } catch (MongoCommandException mce) {
            if (mce.getCode() == 20) {
                logger.debug(String.format("Sharding already enabled for %s", sourceColl.get("_id")));
            } else {
                throw mce;
            }
        }
        return result;
    }
    
    /**
     * 
     * @param sync - THIS WILL shard on the dest side if not in sync
     */
    public void diffShardedCollections(boolean sync) {
        logger.debug("diffShardedCollections()");
        sourceShard.populateCollectionsMap();
        destShard.populateCollectionsMap();
        
        for (Document sourceColl : sourceShard.getCollectionsMap().values()) {
            
            String nsStr = (String)sourceColl.get("_id");
            Namespace ns = new Namespace(nsStr);
            if (filtered && ! namespaceFilters.contains(ns) && !databaseFilters.contains(ns.getDatabaseName())) {
                //logger.debug("Namespace " + ns + " filtered, not sharding on destination");
                continue;
            }
            
            Document destCollection = destShard.getCollectionsMap().get(sourceColl.get("_id"));
            
            if (destCollection == null) {
                logger.debug("Destination collection not found: " + sourceColl.get("_id") + " sourceKey:" + sourceColl.get("key"));
                if (sync) {
                    try {
                        Document result = shardCollection(sourceColl);
                        logger.debug("Sharded: " + result);
                    } catch (MongoCommandException mce) {
                        logger.error("Error sharding", mce);
                    }
                }
            } else {
                if (sourceColl.get("key").equals(destCollection.get("key"))) {
                    logger.debug("Shard key match for " + sourceColl);
                } else {
                    logger.warn("Shard key MISMATCH for " + sourceColl + " sourceKey:" + sourceColl.get("key") + " destKey:" + destCollection.get("key"));
                }
            }
        }
    }
    
    public void enableDestinationSharding() {
        logger.debug("enableDestinationSharding()");
        MongoCollection<Document> databasesColl = sourceShard.getConfigDb().getCollection("databases");
        
        // todo, what about unsharded collections, don't we need to movePrimary for them?
        //FindIterable<Document> databases = databasesColl.find(eq("partitioned", true));
        FindIterable<Document> databases = databasesColl.find();
        
        List<Document> databasesList = new ArrayList<Document>();
        databases.into(databasesList);
        for (Document database : databasesList) {
            String databaseName = database.getString("_id");
            if (databaseName.equals("admin") || databaseName.equals("system") || databaseName.contains("$")) {
                continue;
            }
            String primary = database.getString("primary");
            String mappedPrimary = sourceToDestShardMap.get(primary);
            logger.debug("database: " + databaseName + ", primary: " + primary + ", mappedPrimary: " + mappedPrimary);
//            if (mappedPrimary == null) {
//                throw new IllegalArgumentException("Shard mapping not found for shard " + primary);
//            }
            
            if (filtered && !databaseFilters.contains(databaseName)) {
                logger.debug("Database " + databaseName + " filtered, not sharding on destination");
                continue;
            }
            
            Document dest = destShard.getConfigDb().getCollection("databases").find(new Document("_id", databaseName)).first();
            if (database.getBoolean("partitioned", true)) {
                logger.debug("enableSharding: " + databaseName);
                try {
                    destShard.adminCommand(new Document("enableSharding", databaseName));
                } catch (MongoCommandException mce) {
                    if (mce.getCode() == 23 && mce.getErrorMessage().contains("sharding already enabled")) {
                        logger.debug("Sharding already enabled: " + databaseName);
                    } else {
                        throw mce;
                    }
                }
                
            }
            
            dest = destShard.getDatabasesCollection().find(new Document("_id", databaseName)).first();
            if (dest == null) {
                destShard.createDatabase(databaseName);
                dest = destShard.getDatabasesCollection().find(new Document("_id", databaseName)).first();
            }
            String destPrimary = dest.getString("primary");
            if (mappedPrimary.equals(destPrimary)) {
                logger.debug("Primary shard already matches for database: " + databaseName);
            } else {
                logger.debug("movePrimary for database: " + databaseName + " from " + destPrimary + " to " + mappedPrimary);
                destShard.adminCommand(new Document("movePrimary", databaseName).append("to", mappedPrimary));
            }
            
        }
        logger.debug("enableDestinationSharding() complete");
    }
    
    /**
     * Drop based on config.databases
     */
    public void dropDestinationDatabases() {
        logger.debug("dropDestinationDatabases()");
        destShard.populateShardMongoClients();
        MongoCollection<Document> databasesColl = sourceShard.getDatabasesCollection();
        FindIterable<Document> databases = databasesColl.find();
        List<String> databasesList = new ArrayList<String>();
        
        for (Document database : databases) {
            String databaseName = database.getString("_id");
            
            if (filtered && !databaseFilters.contains(databaseName)) {
                logger.debug("Database " + databaseName + " filtered, not dropping on destination");
                continue;
            } else {
                databasesList.add(databaseName);
            }
        }
        destShard.dropDatabases(databasesList);
        logger.debug("dropDestinationDatabases() complete");
    }
    
    public void dropDestinationDatabasesAndConfigMetadata() {
        logger.debug("dropDestinationDatabasesAndConfigMetadata()");
        destShard.populateShardMongoClients();
        MongoCollection<Document> databasesColl = sourceShard.getDatabasesCollection();
        FindIterable<Document> databases = databasesColl.find();
        List<String> databasesList = new ArrayList<String>();
        
        for (Document database : databases) {
            String databaseName = database.getString("_id");
            
            if (filtered && !databaseFilters.contains(databaseName)) {
                logger.debug("Database " + databaseName + " filtered, not dropping on destination");
                continue;
            } else {
                databasesList.add(databaseName);
            }
        }
        destShard.dropDatabasesAndConfigMetadata(databasesList);
        logger.debug("dropDestinationDatabasesAndConfigMetadata() complete");
        
    }
    
    public void cleanupOrphans() {
        logger.debug("cleanupOrphans()");
        sourceShard.populateCollectionsMap();
        sourceShard.populateShardMongoClients();
        CleanupOrphaned cleaner = new CleanupOrphaned(sourceShard);
        cleaner.cleanupOrphans();
    }
    
    public void cleanupOrphansDest() {
        logger.debug("cleanupOrphansDest()");
        destShard.populateCollectionsMap();
        destShard.populateShardMongoClients();
        CleanupOrphaned cleaner = new CleanupOrphaned(destShard);
        cleaner.cleanupOrphans();
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

    public boolean isDropDestDbs() {
        return dropDestDbs;
    }

    public void setDropDestDbs(boolean dropDestinationCollectionsIfExisting) {
        this.dropDestDbs = dropDestinationCollectionsIfExisting;
    }

    public void setDoChunkCounts(boolean doChunkCounts) {
        this.doChunkCounts = doChunkCounts;
    }
    
    public void setNamespaceFilters(String[] namespaceFilterList) {
        this.namespaceFilterList = namespaceFilterList;
        if (namespaceFilterList == null) {
            return;
        }
        filtered = true;
        for (String nsStr : namespaceFilterList) {
            if (nsStr.contains("\\.")) {
                Namespace ns = new Namespace(nsStr);
                namespaceFilters.add(ns);
                databaseFilters.add(ns.getDatabaseName());
            } else {
                databaseFilters.add(nsStr);
            }
        }
    }
    
    public void setShardMappings(String[] shardMap) {
        this.shardMap = shardMap;
    }
    
    public void shardToRs() throws ExecuteException, IOException {
        
        logger.debug("shardToRs() starting");
        
        for (Shard source : sourceShard.getShardsMap().values()) {
            logger.debug("sourceShard: " + source.getId());
            MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());
            
            // Source setup
            mongomirror.setSourceHost(source.getHost());
            mongomirror.setSourceUsername(sourceShard.getUsername());
            if (sourceShard.getPassword() != null) {
                mongomirror.setSourcePassword(sourceShard.getPassword());
            }
            MongoCredential sourceCredentials = sourceShard.getCredentials();
            if (sourceCredentials != null) {
                mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
            }
            if (sourceShard.getOptions().getSslContext() != null) {
                mongomirror.setSourceSsl(true);
            }
            
            
            String setName = destShard.getMongoClient().getReplicaSetStatus().getName();
            
            
            //destMongoClientURI.getCredentials().getSource();
            String host = destShard.getMongoClient().getAddress().getHost();
            
            mongomirror.setDestinationHost(setName + "/" + host);
            mongomirror.setDestinationUsername(destShard.getUsername());
            if (destShard.getPassword() != null) {
                mongomirror.setDestinationPassword(destShard.getPassword());
            }
            MongoCredential destCredentials = destShard.getCredentials();
            if (destCredentials != null) {
                mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
            }
            if (! destShard.getOptions().isSslEnabled()) {
                mongomirror.setDestinationNoSSL(true);
            }
            
            
            if (namespaceFilterList != null) {
                String nsFilter = String.join(",", namespaceFilterList);
                mongomirror.setNamespaceFilter(nsFilter);
            }
            
            
//            if (dropDestinationCollectionsIfExisting) {
//                if (! destShard.isMongomirrorDropped()) {
//                    // for n:m shard mapping, only set drop on the first mongomiirror that we start,
//                    // since there will be multiple mongomirrors pointing to the same destination
//                    // and we would drop data that had started to copy
//                    mongomirror.setDrop(dropDestinationCollectionsIfExisting);
//                    destShard.setMongomirrorDropped(true);
//                }
//            }
            
            mongomirror.setMongomirrorBinary(mongomirrorBinary);
            mongomirror.setBookmarkFile(source.getId() + ".timestamp");
            mongomirror.setNumParallelCollections(numParallelCollections);
            mongomirror.execute();
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }

    public void mongomirror() throws ExecuteException, IOException {
        
        for (Shard source : sourceShard.getShardsMap().values()) {
            MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());
            
            // Source setup
            mongomirror.setSourceHost(source.getHost());
            mongomirror.setSourceUsername(sourceShard.getUsername());
            if (sourceShard.getPassword() != null) {
                mongomirror.setSourcePassword(String.valueOf(sourceShard.getPassword()));
            }
            MongoCredential sourceCredentials = sourceShard.getCredentials();
            if (sourceCredentials != null) {
                mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
            }
            if (sourceShard.getOptions().isSslEnabled()) {
                mongomirror.setSourceSsl(true);
            }
            
            // Destination setup
            String destShardId = sourceToDestShardMap.get(source.getId());
            Shard dest = destShard.getShardsMap().get(destShardId);
            mongomirror.setDestinationHost(dest.getHost());
            mongomirror.setDestinationUsername(destShard.getUsername());
            if (destShard.getPassword() != null) {
                mongomirror.setDestinationPassword(destShard.getPassword());
            }
            MongoCredential destCredentials = destShard.getCredentials();
            if (destCredentials != null) {
                mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
            }
            if (! destShard.getOptions().isSslEnabled()) {
                mongomirror.setDestinationNoSSL(true);
            }
            
            
            if (namespaceFilterList != null) {
                String nsFilter = String.join(",", namespaceFilterList);
                mongomirror.setNamespaceFilter(nsFilter);
            }
            
            mongomirror.setMongomirrorBinary(mongomirrorBinary);
            mongomirror.setBookmarkFile(source.getId() + ".timestamp");
            mongomirror.setNumParallelCollections(numParallelCollections);
            
            if (destShard.isVersion36OrLater()) {
                logger.debug("Version 3.6 or later, setting preserveUUIDs true");
                mongomirror.setPreserveUUIDs(true);
            }
            if (tailOnly) {
               mongomirror.setTailOnly(tailOnly);
            }
            mongomirror.execute();
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }

    public void setMongomirrorBinary(String binaryPath) {
        this.mongomirrorBinary = new File(binaryPath);
    }

    public void setSleepMillis(String optionValue) {
        if (optionValue != null) {
            this.sleepMillis = Long.parseLong(optionValue);
        }
    }

    public void setNumParallelCollections(String numParallelCollections) {
        this.numParallelCollections = numParallelCollections;
    }

    public void setNonPrivilegedMode(boolean nonPrivilegedMode) {
        this.nonPrivilegedMode = nonPrivilegedMode;
    }

    public void flushRouterConfig() {
        destShard.flushRouterConfig();
    }

    public void setDropDestDbsAndConfigMetadata(boolean dropDestinationConfigMetadata) {
        this.dropDestDbsAndConfigMetadata = dropDestinationConfigMetadata;
    }

    public void setSslAllowInvalidHostnames(boolean sslAllowInvalidHostnames) {
        this.sslAllowInvalidHostnames = sslAllowInvalidHostnames;
    }

    public void setSslAllowInvalidCertificates(boolean sslAllowInvalidCertificates) {
        this.sslAllowInvalidCertificates = sslAllowInvalidCertificates;
    }

    public void setPreserveUUIDs(boolean preserveUUIDs) {
        this.preserveUUIDs = preserveUUIDs;
    }

    public void setTailOnly(boolean tailOnly) {
        this.tailOnly = tailOnly;
    }
}
