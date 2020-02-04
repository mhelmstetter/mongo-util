package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.regex;
import static com.mongodb.client.model.Projections.include;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.internal.dns.DefaultDnsResolver;
import com.mongodb.model.Mongos;
import com.mongodb.model.Shard;

/**
 * This class encapsulates the client related objects needed for each source and
 * destination
 *
 */
public class ShardClient {

    private static Logger logger = LoggerFactory.getLogger(ShardClient.class);

    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";

    private final static List<Document> countPipeline = new ArrayList<Document>();
    static {
        countPipeline.add(Document.parse("{ $group: { _id: null, count: { $sum: 1 } } }"));
        countPipeline.add(Document.parse("{ $project: { _id: 0, count: 1 } }"));
    }

    private String name;
    private String version;
    private List<Integer> versionArray;
    //private MongoClientURI mongoClientURI;
    private MongoClient mongoClient;
    private MongoDatabase configDb;
    private Map<String, Shard> shardsMap = new LinkedHashMap<String, Shard>();
    
    private ConnectionString connectionString;
    private MongoClientSettings mongoClientSettings;
    //private String username;
    //private String password;
    //private MongoClientOptions mongoClientOptions;

    private List<Mongos> mongosList = new ArrayList<Mongos>();
    private Map<String, MongoClient> mongosMongoClients = new TreeMap<String, MongoClient>();

    private Map<String, Document> collectionsMap = new TreeMap<String, Document>();

    private Map<String, MongoClient> shardMongoClients = new TreeMap<String, MongoClient>();
    
    private List<String> srvHosts;
    
    private Collection<String> shardIdFilter;
    
    public ShardClient(String name, String clusterUri, Collection<String> shardIdFilter) {
    	this.name = name;
    	this.shardIdFilter = shardIdFilter;
        logger.debug(String.format("%s client, uri: %s", name, clusterUri));
        
        connectionString = new ConnectionString(clusterUri);
        mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
         
        mongoClient = MongoClients.create(mongoClientSettings);
        
        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));


        //List<ServerAddress> addrs = mongoClient.getServerAddressList();
        adminCommand(new Document("ping", 1));
        // logger.debug("Connected to source");
        configDb = mongoClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        populateShardList();

        Document destBuildInfo = adminCommand(new Document("buildinfo", 1));
        version = destBuildInfo.getString("version");
        versionArray = (List<Integer>) destBuildInfo.get("versionArray");
        logger.debug(name + ": MongoDB version: " + version);

        populateMongosList();
    }

    public ShardClient(String name, String clusterUri) {
    	this(name, clusterUri, null);
    }

    private void populateShardList() {

        MongoCollection<Shard> shardsColl = configDb.getCollection("shards", Shard.class);
        FindIterable<Shard> shards = shardsColl.find().sort(Sorts.ascending("_id"));
        for (Shard sh : shards) {
        	if (shardIdFilter != null && ! shardIdFilter.contains(sh.getId())) {
        		continue;
        	}
            shardsMap.put(sh.getId(), sh);
        }
        logger.debug(name + ": populateShardList complete, " + shardsMap.size() + " shards added");
    }

    private void populateMongosList() {
        
        if (connectionString.isSrvProtocol()) {
            
            DefaultDnsResolver resolver = new DefaultDnsResolver();
            srvHosts = resolver.resolveHostFromSrvRecords(connectionString.getHosts().get(0));
            
            for (String hostPort : srvHosts) {
                logger.debug("populateMongosList() mongos srvHost: " + hostPort);
                
                String host = StringUtils.substringBefore(hostPort, ":");
                Integer port = Integer.parseInt(StringUtils.substringAfter(hostPort, ":"));
                
                MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
                settingsBuilder.applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress(host, port))));
                if (connectionString.getSslEnabled() != null) {
                    settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
                }
                if (connectionString.getCredential() != null) {
                    settingsBuilder.credential(connectionString.getCredential());
                }
                MongoClientSettings settings = settingsBuilder.build();

                MongoClient mongoClient = MongoClients.create(settings);
                mongosMongoClients.put(hostPort, mongoClient);
            }
            
        } else {
            MongoCollection<Mongos> mongosColl = configDb.getCollection("mongos", Mongos.class);
            // LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);

            // TODO this needs to take into account "dead" mongos instances
            int limit = 9999;
            if (name.equals("source")) {
                limit = 5;
            }
            mongosColl.find().sort(Sorts.ascending("ping")).limit(limit).into(mongosList);
            for (Mongos mongos : mongosList) {
                
                logger.debug(name + " mongos: " + mongos.getId());
                
                String hostPort = mongos.getId();
                String host = StringUtils.substringBefore(hostPort, ":");
                Integer port = Integer.parseInt(StringUtils.substringAfter(hostPort, ":"));
                
                MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
                settingsBuilder.applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(new ServerAddress(host, port))));
                if (connectionString.getSslEnabled() != null) {
                    settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
                }
                if (connectionString.getCredential() != null) {
                    settingsBuilder.credential(connectionString.getCredential());
                }
                MongoClientSettings settings = settingsBuilder.build();
                
                MongoClient mongoClient = MongoClients.create(settings);
                mongosMongoClients.put(mongos.getId(), mongoClient);
            }
               
        }
        

        
        logger.debug(name + " populateMongosList complete, " + mongosMongoClients.size() + " mongosMongoClients added");
    }
    
    /**
     *  Populate only a subset of all collections. Useful if there are a very large number of
     *  namespaces present.
     */
    public void populateCollectionsMap(Set<String> namespaces) {
        logger.debug("Starting populateCollectionsMap()");
        MongoCollection<Document> shardsColl = configDb.getCollection("collections");
        Bson filter = null;
        if (namespaces == null || namespaces.isEmpty()) {
            filter = eq("dropped", false);
        } else {
            filter = and(eq("dropped", false), in("_id", namespaces));
        }
        FindIterable<Document> colls = shardsColl.find(filter).sort(Sorts.ascending("_id"));
        for (Document c : colls) {
            String id = (String)c.get("_id");
            collectionsMap.put(id, c);
        }
        logger.debug(String.format("Finished populateCollectionsMap(), %s collections loaded from config server", collectionsMap.size()));
    }

    public void populateCollectionsMap() {
        populateCollectionsMap(null);
    }

    public void populateShardMongoClients() {
        // MongoCredential sourceCredentials = mongoClientURI.getCredentials();
    	
    	if (shardMongoClients.size() > 0) {
    		logger.debug("populateShardMongoClients already complete, skipping");
    	}

        for (Shard shard : shardsMap.values()) {
            String shardHost = shard.getHost();
            String seeds = StringUtils.substringAfter(shardHost, "/");
            
            logger.debug(name + " " + shard.getId() + " populateShardMongoClients() seeds: " + seeds);
            
            String[] seedHosts = seeds.split(",");
            
            List<ServerAddress> serverAddressList = new ArrayList<>();
            for (String seed : seedHosts) {
                String host = StringUtils.substringBefore(seed, ":");
                Integer port = Integer.parseInt(StringUtils.substringAfter(seed, ":"));
                
                serverAddressList.add(new ServerAddress(host, port));
            }
            
            MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
            settingsBuilder.applyToClusterSettings(builder -> builder.hosts(serverAddressList));
            if (connectionString.getSslEnabled() != null) {
                settingsBuilder.applyToSslSettings(builder -> builder.enabled(connectionString.getSslEnabled()));
            }
            if (connectionString.getCredential() != null) {
                settingsBuilder.credential(connectionString.getCredential());
            }
            MongoClientSettings settings = settingsBuilder.build();
            MongoClient mongoClient = MongoClients.create(settings);
            
            logger.debug(String.format("%s isMaster started: %s", name, shardHost));
            Document isMasterResult = mongoClient.getDatabase("admin").runCommand(new Document("isMaster", 1));
            if (logger.isTraceEnabled()) {
                logger.trace(name + " isMaster complete, cluster: " + mongoClient.getClusterDescription());
            } else {
                logger.debug(String.format("%s isMaster complete: %s", name, shardHost));
            }
            
            shardMongoClients.put(shard.getId(), mongoClient);
        }
    }

    public BsonTimestamp getLatestOplogTimestamp(String shardId) {
        MongoClient client = shardMongoClients.get(shardId);
        MongoCollection<Document> coll = client.getDatabase("local").getCollection("oplog.rs");
        Document doc = coll.find().projection(include("ts")).sort(eq("$natural", -1)).first();
        return (BsonTimestamp)doc.get("ts");
    }

    /**
     * This will drop the db on each shard, config data will NOT be touched
     * 
     * @param dbName
     */
    public void dropDatabase(String dbName) {
        for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {
            logger.debug(name + " dropping " + dbName + " on " + entry.getKey());
            entry.getValue().getDatabase(dbName).drop();
        }
    }

    /**
     * This will drop the db on each shard, config data will NOT be touched
     * 
     * @param dbName
     */
    public void dropDatabases(List<String> databasesList) {
        for (Map.Entry<String, MongoClient> entry : shardMongoClients.entrySet()) {
            
            for (String dbName : databasesList) {
                if (! dbName.equals("admin")) {
                    logger.debug(name + " dropping " + dbName + " on " + entry.getKey());
                    entry.getValue().getDatabase(dbName).drop();
                }
                
            }
        }
    }
    
    private void dropForce(String dbName) {
        DeleteResult r = mongoClient.getDatabase("config").getCollection("collections").deleteMany(regex("_id", "^" + dbName + "\\."));
        logger.debug(String.format("Force deleted %s config.collections documents", r.getDeletedCount()));
        r = mongoClient.getDatabase("config").getCollection("chunks").deleteMany(regex("ns", "^" + dbName + "\\."));
        logger.debug(String.format("Force deleted %s config.chunks documents", r.getDeletedCount()));
    }

    public void dropDatabasesAndConfigMetadata(List<String> databasesList) {
        for (String dbName : databasesList) {
            if (! dbName.equals("admin")) {
                logger.debug(name + " dropping " + dbName);
                try {
                    mongoClient.getDatabase(dbName).drop();
                } catch (MongoCommandException mce) {
                    logger.debug("Drop failed, brute forcing.");
                    dropForce(dbName);
                }
               
            }
        }
    }
    
    public static Number getFastCollectionCount(MongoDatabase db, MongoCollection<RawBsonDocument> collection) {
        return collection.countDocuments();
    }
    
    public static Number getCollectionCount(MongoDatabase db, MongoCollection<RawBsonDocument> collection) {
        BsonDocument result = collection.aggregate(countPipeline).first();
        Number count = null;
        if (result != null) {
            count = result.get("count").asNumber().longValue();
        }
        return count;
    }

    public static Number getCollectionCount(MongoDatabase db, String collectionName) {
        return getCollectionCount(db, db.getCollection(collectionName, RawBsonDocument.class));
    }
    
    public MongoCollection<Document> getShardsCollection() {
        return configDb.getCollection("shards");
    }
    
    public MongoCollection<Document> getTagsCollection() {
        return configDb.getCollection("tags");
    }

    public MongoCollection<Document> getChunksCollection() {
        return configDb.getCollection("chunks");
    }
    
    public MongoCollection<RawBsonDocument> getChunksCollectionRaw() {
        return configDb.getCollection("chunks", RawBsonDocument.class);
    }

    public MongoCollection<Document> getDatabasesCollection() {
        return configDb.getCollection("databases");
    }

    public void createDatabase(String databaseName) {
    	logger.debug(name + " createDatabase " + databaseName);
        String tmpName = "tmp_ShardConfigSync_" + System.currentTimeMillis();
        mongoClient.getDatabase(databaseName).createCollection(tmpName);
        
        // ugly hack
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        mongoClient.getDatabase(databaseName).getCollection(tmpName).drop();
    }
    
//    public void createDatabaseOnShards(String databaseName) {
//    	logger.debug(String.format("%s - createDatabaseOnShards(): %s", name, databaseName));
//    	
//    	
//        String tmpName = "tmp_ShardConfigSync_" + System.currentTimeMillis();
//        mongoClient.getDatabase(databaseName).createCollection(tmpName);
//        
//        // ugly hack
//        try {
//            Thread.sleep(1000);
//        } catch (InterruptedException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }
//        mongoClient.getDatabase(databaseName).getCollection(tmpName).drop();
//    }
    
    public MongoIterable<String> listDatabaseNames() {
        return this.mongoClient.listDatabaseNames();
    }

    public MongoIterable<String> listCollectionNames(String databaseName) {
        return this.mongoClient.getDatabase(databaseName).listCollectionNames();
    }

    public void flushRouterConfig() {
        logger.debug(String.format("flushRouterConfig() for %s mongos routers", mongosMongoClients.size()));
        for (Map.Entry<String, MongoClient> entry : mongosMongoClients.entrySet()) {
            MongoClient client = entry.getValue();
            Document flushRouterConfig = new Document("flushRouterConfig", true);

            try {
                logger.debug(String.format("flushRouterConfig for mongos %s", entry.getKey()));
                client.getDatabase("admin").runCommand(flushRouterConfig);
            } catch (MongoTimeoutException timeout) {
                logger.debug("Timeout connecting", timeout);
            }
        }
    }

    public void stopBalancer() {
        if (versionArray.get(0) == 2 || (versionArray.get(0) == 3 && versionArray.get(1) <= 2)) {
            Document balancerId = new Document("_id", "balancer");
            Document setStopped = new Document("$set", new Document("stopped", true));
            UpdateOptions updateOptions = new UpdateOptions().upsert(true);
            configDb.getCollection("settings").updateOne(balancerId, setStopped, updateOptions);
        } else {
            adminCommand(new Document("balancerStop", 1));
        }
    }

    public Document adminCommand(Document command) {
        return mongoClient.getDatabase("admin").runCommand(command);
    }

    public String getVersion() {
        return version;
    }

    public List<Integer> getVersionArray() {
        return versionArray;
    }
    
    public boolean isVersion36OrLater() {
        if (versionArray.get(0) >= 4 || (versionArray.get(0) == 3 && versionArray.get(1) == 6)) {
            return true;
        }
        return false;
    }

    public Map<String, Shard> getShardsMap() {
        return shardsMap;
    }

    public Map<String, Document> getCollectionsMap() {
        return collectionsMap;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoDatabase getConfigDb() {
        return configDb;
    }

    public Collection<MongoClient> getMongosMongoClients() {
        return mongosMongoClients.values();
    }
    
    public MongoClient getShardMongoClient(String shardId) {
        return shardMongoClients.get(shardId);
    }
    
    public Map<String, MongoClient> getShardMongoClients() {
        return shardMongoClients;
    }

    public void checkAutosplit() {
        logger.debug(String.format("checkAutosplit() for %s mongos routers", mongosMongoClients.size()));
        for (Map.Entry<String, MongoClient> entry : mongosMongoClients.entrySet()) {
            MongoClient client = entry.getValue();
            Document getCmdLine = new Document("getCmdLineOpts", true);
            Boolean autoSplit = null;
            try {
                // logger.debug(String.format("flushRouterConfig for mongos %s",
                // client.getAddress()));
                Document result = adminCommand(getCmdLine);
                Document parsed = (Document) result.get("parsed");
                Document sharding = (Document) parsed.get("sharding");
                if (sharding != null) {
                    sharding.getBoolean("autoSplit");
                }
                if (autoSplit != null && !autoSplit) {
                    logger.debug("autoSplit disabled for " + entry.getKey());
                } else {
                    logger.warn("autoSplit NOT disabled for " + entry.getKey());
                }
            } catch (MongoTimeoutException timeout) {
                logger.debug("Timeout connecting", timeout);
            }
        }
    }
    
    public void disableAutosplit() {
    	MongoDatabase configDb = mongoClient.getDatabase("config");
    	MongoCollection<RawBsonDocument> settings = configDb.getCollection("settings", RawBsonDocument.class);
    	Document update = new Document("$set", new Document("enabled", false));
    	settings.updateOne(eq("_id", "autosplit"), update);
    }


    public ConnectionString getConnectionString() {
        return connectionString;
    }

}
