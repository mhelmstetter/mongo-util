package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.ne;
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
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.types.MaxKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Mongos;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;
import com.mongodb.mongomirror.MongoMirrorRunner;

// TODO - look at https://github.com/10gen/scripts-and-snippets/blob/master/mongod/recreate-splits.js
public class ShardConfigSync {

    private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);

    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";
    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");

    private String sourceClusterUri;

    private String destClusterUri;
    private String destUsername;
    private String destPassword;
    private MongoClientOptions destOptions;

    private boolean dropDestinationCollectionsIfExisting;
    private boolean doChunkCounts;

    private MongoClientURI sourceMongoClientURI;
    private MongoClient sourceClient;

    private MongoClientURI destMongoClientURI;
    private MongoClient destClient;

    private MongoDatabase sourceConfigDb;
    private MongoDatabase destConfigDb;

    CodecRegistry pojoCodecRegistry;

    private Map<String, Shard> sourceShards = new HashMap<String, Shard>();
    private Map<String, Shard> destShards = new HashMap<String, Shard>();
    private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();

    private List<ShardCollection> sourceCollections = new ArrayList<ShardCollection>();

    private List<Mongos> destMongos = new ArrayList<Mongos>();
    private List<MongoClient> destMongoClients = new ArrayList<MongoClient>();

    private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
    private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();
    
    String[] namespaceFilterList;
    private Set<Namespace> namespaceFilters = new HashSet<Namespace>();
    private Set<String> databaseFilters = new HashSet<String>();
    
    private String[] shardMap;
    
    private File mongomirrorBinary;

    public ShardConfigSync() {
        logger.debug("ShardConfigSync starting");
    }

    public void init() {

        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        sourceMongoClientURI = new MongoClientURI(sourceClusterUri);
        sourceClient = new MongoClient(sourceMongoClientURI);
        
        
        List<ServerAddress> addrs = sourceClient.getServerAddressList();
        sourceClient.getDatabase("admin").runCommand(new Document("ping", 1));
        logger.debug("Connected to source");
        sourceConfigDb = sourceClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);

        boolean isSRVProtocol = destClusterUri.startsWith(MONGODB_SRV_PREFIX);
        if (isSRVProtocol) {
            throw new RuntimeException(
                    "mongodb+srv protocol not supported use standard mongodb protocol in connection string");
        }

        destMongoClientURI = new MongoClientURI(destClusterUri);
        this.destUsername = destMongoClientURI.getUsername();
        if (destUsername != null) {
            this.destPassword = String.valueOf(destMongoClientURI.getPassword());
        }
        
        this.destOptions = destMongoClientURI.getOptions();

        // We need to ensure a consistent connection to only a single mongos
        // assume that we will only use the first one in the list
        if (destMongoClientURI.getHosts().size() > 1) {
            throw new RuntimeException("Specify only a single destination mongos in the connection string");
        }

        destClient = new MongoClient(destMongoClientURI);

        destClient.getDatabase("admin").runCommand(new Document("ping", 1));
        destConfigDb = destClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);

        populateShardList(sourceConfigDb, sourceShards);
        populateShardList(destConfigDb, destShards);

        populateMongosList(destConfigDb, destMongos);
        
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
            // default, just match up the shards 1:1
            int index = 0;
            for (Iterator<Shard> i = sourceShards.values().iterator(); i.hasNext();) {
                Shard sourceShard = i.next();
                Shard destShard = destShards.get(index);
                logger.debug(sourceShard.getId() + " ==> " + destShard.getId());
                sourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
                index++;
            }
        }
    }

    public void migrateMetadata() throws InterruptedException {
        logger.debug("Starting metadata sync/migration");

        stopBalancers();
        // TODO disableAutoSplit !!!!
        enableDestinationSharding();

        populateCollectionList(sourceConfigDb, sourceCollections);
        shardDestinationCollections();

        createDestChunks();

        if (!compareAndMoveChunks(true)) {
            throw new RuntimeException("chunks don't match");
        }

        flushRouterConfig();
    }

    private void stopBalancers() {
        logger.debug("stopBalancers started");
        sourceClient.getDatabase("admin").runCommand(new Document("balancerStop", 1));
        destClient.getDatabase("admin").runCommand(new Document("balancerStop", 1));
        logger.debug("stopBalancers complete");
    }

    private void createDestChunks() {
        logger.debug("createDestChunks started");
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "max"));

        Document splitCommand = new Document();
        String lastNs = null;
        int currentCount = 0;

        for (Iterator<Document> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {
            
            Document chunk = sourceChunksIterator.next();
            
            String ns = chunk.getString("ns");
            Namespace sourceNs = new Namespace(ns);
            
            if (! namespaceFilters.contains(sourceNs) && !databaseFilters.contains(sourceNs.getDatabaseName())) {
                continue;
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
                destClient.getDatabase("admin").runCommand(splitCommand);
            } catch (MongoCommandException mce) {
                logger.error(String.format("command error for namespace %s", ns), mce);
            }

            lastNs = ns;
            currentCount++;
        }
        logger.debug("createDestChunks complete");
    }

    public void compareChunks() {
        compareAndMoveChunks(false);
    }

    private boolean compareAndMoveChunks(boolean doMove) {
        
        logger.debug("Reading destination chunks");
        Map<String, String> destChunkMap = new HashMap<String, String>();
        MongoCollection<Document> destChunksColl = destConfigDb.getCollection("chunks");
        FindIterable<Document> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min"));
       
        for (Document destChunk : destChunks) {
            String id  = destChunk.getString("_id");
            String shard = destChunk.getString("shard");
            destChunkMap.put(id, shard);
        }
        logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
        
        
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find().sort(Sorts.ascending("ns", "min"));

        String lastNs = null;
        int currentCount = 0;
        int movedCount = 0;
        int mismatchedCount = 0;
        int matchedCount = 0;

        for (Document sourceChunk : sourceChunks) {

            String sourceNs = sourceChunk.getString("ns");
            Namespace sourceNamespace = new Namespace(sourceNs);
            
            if (! namespaceFilters.contains(sourceNamespace) && !databaseFilters.contains(sourceNamespace.getDatabaseName())) {
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
                }

                movedCount++;
            }

            lastNs = sourceNs;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    public void compareShardCounts() {

        logger.debug("Starting compareShardCounts mode");

        Document listDatabases = new Document("listDatabases", 1);
        Document sourceDatabases = sourceClient.getDatabase("admin").runCommand(listDatabases);
        Document destDatabases = destClient.getDatabase("admin").runCommand(listDatabases);

        List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
        populateDbMap(destDatabaseInfo, destDbInfoMap);

        for (Document sourceInfo : sourceDatabaseInfo) {
            String dbName = sourceInfo.getString("name");
            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo != null) {
                logger.debug(String.format("Found matching database %s", dbName));

                MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
                MongoDatabase destDb = destClient.getDatabase(dbName);
                MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
                for (String collectionName : sourceCollectionNames) {
                    if (collectionName.equals("system.profile") || collectionName.equals("system.keys")) {
                        continue;
                    }
                    long sourceCount = sourceDb.getCollection(collectionName).count(ne("_id", 0));
                    long destCount = destDb.getCollection(collectionName).count(ne("_id", 0));
                    if (sourceCount == destCount) {
                        logger.debug(String.format("%s.%s count matches: %s", dbName, collectionName, sourceCount));
                    } else {
                        logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s", dbName, collectionName,
                                sourceCount, destCount));
                        if (doChunkCounts) {
                            compareChunkCounts(sourceDb, destDb, collectionName);
                        }
                    }
                }
            } else {
                logger.warn(String.format("Destination db not found, name: %s", dbName));
            }
        }
        System.out.println(destDatabases);

    }

    // TODO - this is incomplete
    private void compareChunkCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
        String ns = sourceDb.getName() + "." + collectionName;
        MongoCollection<Document> sourceChunksColl = sourceConfigDb.getCollection("chunks");
        FindIterable<Document> sourceChunks = sourceChunksColl.find(eq("ns", ns)).sort(Sorts.ascending("ns", "min"));

        MongoCollection<Document> destChunksColl = destConfigDb.getCollection("chunks");
        Iterator<Document> destChunks = destChunksColl.find(eq("ns", ns)).sort(Sorts.ascending("ns", "min")).iterator();

    }

    private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
        for (Document dbInfo : dbInfoList) {
            databaseMap.put(dbInfo.getString("name"), dbInfo);
        }
    }

    public void flushRouterConfig() {
        logger.debug(String.format("flushRouterConfig() for %s mongos routers", destMongoClients.size()));
        for (MongoClient client : destMongoClients) {
            Document flushRouterConfig = new Document("flushRouterConfig", true);
            
            try {
                logger.debug(String.format("flushRouterConfig for mongos %s", client.getAddress()));
                client.getDatabase("admin").runCommand(flushRouterConfig);
            } catch (MongoTimeoutException timeout) {
                logger.debug("Timeout connecting", timeout);
            }
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
        // LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);

        // TODO this needs to take into account "dead" mongos instances
        mongosColl.find().sort(Sorts.ascending("ping")).into(list);
        for (Mongos mongos : destMongos) {
            String uri = null;
            if (destUsername != null && destPassword != null) {
                uri = "mongodb://" + destUsername + ":" + destPassword + "@" + mongos.getId();
            } else {
                uri = "mongodb://" + mongos.getId();
            }
            
            MongoClientOptions.Builder builder = new MongoClientOptions.Builder(destOptions);
            MongoClientURI clientUri = new MongoClientURI(uri, builder);
            logger.debug("mongos: " + clientUri);
            MongoClient client = new MongoClient(clientUri);
            destMongoClients.add(client);
        }
    }

    private void shardDestinationCollections() {
        logger.debug("shardDestinationCollections() started");
        for (ShardCollection sourceColl : sourceCollections) {
            
            Namespace ns = sourceColl.getNamespace();
            
            if (! namespaceFilters.contains(ns) && !databaseFilters.contains(ns.getDatabaseName())) {
                logger.debug("Namespace " + ns + " filtered, not sharding on destination");
                continue;
            }
            
            Document shardCommand = new Document("shardCollection", sourceColl.getId());
            shardCommand.append("key", sourceColl.getKey());
            shardCommand.append("unique", sourceColl.isUnique());
            if (sourceColl.getDefaultCollation() != null) {
                shardCommand.append("collation", LOCALE_SIMPLE);
            }
            try {
                destClient.getDatabase("admin").runCommand(shardCommand);
            } catch (MongoCommandException mce) {
                if (mce.getCode() == 20) {
                    logger.debug(String.format("Sharding already enabled for %s", sourceColl.getId()));
                } else {
                    throw mce;
                }
            }

            if (sourceColl.isNoBalance()) {
                // TODO there is no disableBalancing command so this is not
                // possible in Atlas
                // destClient.getDatabase("admin").runCommand(new Document("",
                // ""));
                logger.warn(String.format("Balancing is disabled for %s, this is not possible in Atlas",
                        sourceColl.getId()));
            }
        }
        logger.debug("shardDestinationCollections() complete");
    }

    private void populateShardList(MongoDatabase db, Map<String, Shard> shardMap) {
        MongoCollection<Shard> shardsColl = db.getCollection("shards", Shard.class);
        FindIterable<Shard> shards = shardsColl.find().sort(Sorts.ascending("_id"));
        for (Shard sh : shards) {
            shardMap.put(sh.getId(), sh);
        }
    }

    public void enableDestinationSharding() {
        logger.debug("enableDestinationSharding()");
        MongoCollection<Document> databasesColl = sourceConfigDb.getCollection("databases");
        FindIterable<Document> databases = databasesColl.find(eq("partitioned", true));
        List<Document> databasesList = new ArrayList<Document>();
        databases.into(databasesList);
        for (Document database : databasesList) {
            String databaseName = database.getString("_id");
            
            if (!databaseFilters.contains(databaseName)) {
                logger.debug("Database " + databaseName + " filtered, not sharding on destination");
                continue;
            }
            
            Document dest = destConfigDb.getCollection("databases").find(new Document("_id", databaseName)).first();
            if (dest == null || !dest.getBoolean("partitioned", false)) {
                destClient.getDatabase("admin").runCommand(new Document("enableSharding", databaseName));
            } else {
                if (dropDestinationCollectionsIfExisting) {
                    logger.debug(String.format("Destination database %s exists, dropping", databaseName));
                    destClient.dropDatabase(databaseName);
                    destClient.getDatabase("admin").runCommand(new Document("enableSharding", databaseName));
                } else {
                    logger.debug(String.format("Sharding already enabled for destination database %s", databaseName));
                }
            }
        }
        logger.debug("enableDestinationSharding() complete");
    }
    
    public void dropDestinationDatabases() {
        logger.debug("dropDestinationDatabases()");
        MongoCollection<Document> databasesColl = sourceConfigDb.getCollection("databases");
        FindIterable<Document> databases = databasesColl.find();
        List<String> databasesList = new ArrayList<String>();
        
        for (Document database : databases) {
            String databaseName = database.getString("_id");
            
            if (!databaseFilters.contains(databaseName)) {
                logger.debug("Database " + databaseName + " filtered, not sharding on destination");
                continue;
            } else {
                databasesList.add(databaseName);
            }
        }
            
            
        for (Shard destShard : destShards.values()) {
            MongoClientOptions.Builder builder = MongoClientOptions.builder();
            if (destMongoClientURI.getOptions().isSslEnabled()) {
                builder.sslEnabled(true);
            }
            MongoClientOptions options = builder
                    .serverSelectionTimeout(60000)
                    .build();
            List<ServerAddress> mongoHosts = new ArrayList<ServerAddress>();
            String hostList = StringUtils.substringAfter(destShard.getHost(), "/");
            String[] hosts = hostList.split(",");
            for (String host : hosts) {
                mongoHosts.add(new ServerAddress(host));
            }
            MongoClient c = null;
            if (destMongoClientURI.getCredentials() != null) {
                c = new MongoClient(mongoHosts, destMongoClientURI.getCredentials(), options);
            } else {
                c = new MongoClient(mongoHosts, options);
            }
            
            for (String dbName : databasesList) {
                logger.debug("Dropping " + dbName + " on " + destShard.getHost());
                c.dropDatabase(dbName);
            }
            c.close();
        }            
        
        logger.debug("dropDestinationDatabases() complete");
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

    public void setDoChunkCounts(boolean doChunkCounts) {
        this.doChunkCounts = doChunkCounts;
    }
    
    public void setNamespaceFilters(String[] namespaceFilterList) {
        this.namespaceFilterList = namespaceFilterList;
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

    public void mongomirror() throws ExecuteException, IOException {
        
        if (dropDestinationCollectionsIfExisting) {
           dropDestinationDatabases();
        }
        
        for (Shard sourceShard : sourceShards.values()) {
            MongoMirrorRunner mongomirror = new MongoMirrorRunner(sourceShard.getId());
            
            // Source setup
            mongomirror.setSourceHost(sourceShard.getHost());
            mongomirror.setSourceUsername(sourceMongoClientURI.getUsername());
            if (sourceMongoClientURI.getPassword() != null) {
                mongomirror.setSourcePassword(String.valueOf(sourceMongoClientURI.getPassword()));
            }
            MongoCredential sourceCredentials = sourceMongoClientURI.getCredentials();
            if (sourceCredentials != null) {
                mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
            }
            if (sourceMongoClientURI.getOptions().getSslContext() != null) {
                mongomirror.setSourceSsl(true);
            }
            
            // Destination setup
            String destShardId = sourceToDestShardMap.get(sourceShard.getId());
            Shard destShard = destShards.get(destShardId);
            mongomirror.setDestinationHost(destShard.getHost());
            mongomirror.setDestinationUsername(destMongoClientURI.getUsername());
            if (destMongoClientURI.getPassword() != null) {
                mongomirror.setDestinationPassword(String.valueOf(destMongoClientURI.getPassword()));
            }
            MongoCredential destCredentials = destMongoClientURI.getCredentials();
            if (destCredentials != null) {
                mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
            }
            if (! destMongoClientURI.getOptions().isSslEnabled()) {
                mongomirror.setDestinationNoSSL(true);
            }
            
            
            String nsFilter = String.join(",", namespaceFilterList);
            mongomirror.setNamespaceFilter(nsFilter);
            
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
            mongomirror.setBookmarkFile(sourceShard.getId() + ".timestamp");
            mongomirror.execute();
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        
    }

    public void setMongomirrorBinary(String binaryPath) {
        this.mongomirrorBinary = new File(binaryPath);
    }
}
