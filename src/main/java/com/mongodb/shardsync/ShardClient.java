package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoCredential;
import com.mongodb.MongoTimeoutException;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.model.Mongos;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;

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
    private MongoClientURI mongoClientURI;
    private MongoClient mongoClient;
    private MongoDatabase configDb;
    private Map<String, Shard> shardsMap = new LinkedHashMap<String, Shard>();
    private String username;
    private String password;
    private MongoClientOptions mongoClientOptions;

    private List<Mongos> mongosList = new ArrayList<Mongos>();
    private List<MongoClient> mongosMongoClients = new ArrayList<MongoClient>();

    private Map<String, ShardCollection> collectionsMap = new TreeMap<String, ShardCollection>();

    private Map<String, MongoClient> shardMongoClients = new TreeMap<String, MongoClient>();

    public ShardClient(String name, String clusterUri) {
        this.name = name;
        this.mongoClientURI = new MongoClientURI(clusterUri);
        
        logger.debug(String.format("%s client, uri: %s", name, clusterUri));

        this.username = mongoClientURI.getUsername();
        if (username != null) {
            this.password = String.valueOf(mongoClientURI.getPassword());
        }

        this.mongoClientOptions = mongoClientURI.getOptions();

        boolean isSRVProtocol = clusterUri.startsWith(MONGODB_SRV_PREFIX);
        if (isSRVProtocol) {
            throw new RuntimeException(
                    "mongodb+srv protocol not supported use standard mongodb protocol in connection string");
        }

        // We need to ensure a consistent connection to only a single mongos
        // assume that we will only use the first one in the list
        if (mongoClientURI.getHosts().size() > 1) {
            throw new RuntimeException("Specify only a single mongos in the connection string");
        }

        mongoClient = new MongoClient(mongoClientURI);

        CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        List<ServerAddress> addrs = mongoClient.getServerAddressList();
        mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
        // logger.debug("Connected to source");
        configDb = mongoClient.getDatabase("config").withCodecRegistry(pojoCodecRegistry);
        populateShardList();

        Document destBuildInfo = mongoClient.getDatabase("admin").runCommand(new Document("buildinfo", 1));
        version = destBuildInfo.getString("version");
        versionArray = (List<Integer>) destBuildInfo.get("versionArray");
        logger.debug(name + ": MongoDB version: " + version);

        populateMongosList();
    }

    private void populateShardList() {

        MongoCollection<Shard> shardsColl = configDb.getCollection("shards", Shard.class);
        FindIterable<Shard> shards = shardsColl.find().sort(Sorts.ascending("_id"));
        for (Shard sh : shards) {
            shardsMap.put(sh.getId(), sh);
        }
        logger.debug(name + ": populateShardList complete, " + shardsMap.size() + " shards added");
    }

    private void populateMongosList() {
        MongoCollection<Mongos> mongosColl = configDb.getCollection("mongos", Mongos.class);
        // LocalDateTime oneHourAgo = LocalDateTime.now().minusHours(1);

        // TODO this needs to take into account "dead" mongos instances
        int limit = 9999;
        if (name.equals("source")) {
            limit = 5;
        }
        mongosColl.find().sort(Sorts.ascending("ping")).limit(limit).into(mongosList);
        for (Mongos mongos : mongosList) {
            String uri = null;
            if (username != null && password != null) {
                uri = "mongodb://" + username + ":" + password + "@" + mongos.getId();
            } else {
                uri = "mongodb://" + mongos.getId();
            }

            MongoClientOptions.Builder builder = new MongoClientOptions.Builder(mongoClientOptions);
            MongoClientURI clientUri = new MongoClientURI(uri, builder);
            logger.debug(name + " mongos: " + clientUri);
            MongoClient client = new MongoClient(clientUri);
            mongosMongoClients.add(client);
        }
        logger.debug(name + " populateMongosList complete, " + mongosMongoClients.size() + " mongosMongoClients added");
    }
    
    /**
     *  Populate only a subset of all collections. Useful if there are a very large number of
     *  namespaces present.
     */
    public void populateCollectionsMap(Set<String> namespaces) {
        logger.debug("Starting populateCollectionsMap()");
        MongoCollection<ShardCollection> shardsColl = configDb.getCollection("collections", ShardCollection.class);
        Bson filter = null;
        if (namespaces == null || namespaces.isEmpty()) {
            filter = eq("dropped", false);
        } else {
            filter = and(eq("dropped", false), in("_id", namespaces));
        }
        FindIterable<ShardCollection> colls = shardsColl.find(filter).sort(Sorts.ascending("_id"));
        for (ShardCollection c : colls) {
            collectionsMap.put(c.getId(), c);
        }
        logger.debug(String.format("Finished populateCollectionsMap(), %s collections loaded from config server", collectionsMap.size()));
    }

    public void populateCollectionsMap() {
        populateCollectionsMap(null);
    }

    public void populateShardMongoClients() {
        // MongoCredential sourceCredentials = mongoClientURI.getCredentials();

        for (Shard shard : shardsMap.values()) {
            String host = shard.getHost();
            String seeds = StringUtils.substringAfter(host, "/");
            String uri = null;
            if (username != null && password != null) {
                uri = "mongodb://" + username + ":" + password + "@" + seeds;
            } else {
                uri = "mongodb://" + seeds;
            }

            MongoClientOptions.Builder builder = new MongoClientOptions.Builder(mongoClientOptions);
            MongoClientURI clientUri = new MongoClientURI(uri, builder);
            MongoClient client = new MongoClient(clientUri);
            
            logger.debug("Start ping: " + uri);
            client.getDatabase("admin").runCommand(new Document("ping", 1));
            logger.debug(name + " connected to shard host: " + host);
            shardMongoClients.put(shard.getId(), client);
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
        for (MongoClient c : shardMongoClients.values()) {
            logger.debug(name + " dropping " + dbName + " on " + c.getConnectPoint());
            c.dropDatabase(dbName);
        }
    }

    /**
     * This will drop the db on each shard, config data will NOT be touched
     * 
     * @param dbName
     */
    public void dropDatabases(List<String> databasesList) {
        for (MongoClient c : shardMongoClients.values()) {
            for (String dbName : databasesList) {
                logger.debug(name + " dropping " + dbName + " on " + c.getConnectPoint());
                c.dropDatabase(dbName);
            }
        }
    }

    public void dropDatabasesAndConfigMetadata(List<String> databasesList) {
        MongoClient c = mongosMongoClients.get(0);
        for (String dbName : databasesList) {
            logger.debug(name + " dropping " + dbName + " using mongos " + c.getConnectPoint());
            c.dropDatabase(dbName);
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

    public MongoCollection<Document> getChunksCollection() {
        return configDb.getCollection("chunks");
    }

    public MongoCollection<Document> getDatabasesCollection() {
        return configDb.getCollection("databases");
    }

    public void createDatabase(String databaseName) {
        String tmpName = "tmp_ShardConfigSync_" + System.currentTimeMillis();
        mongoClient.getDatabase(databaseName).createCollection(tmpName);
        mongoClient.getDatabase(databaseName).getCollection(tmpName).drop();
    }

    public MongoIterable<String> listCollectionNames(String databaseName) {
        return this.mongoClient.getDatabase(databaseName).listCollectionNames();
    }

    public void flushRouterConfig() {
        logger.debug(String.format("flushRouterConfig() for %s mongos routers", mongosMongoClients.size()));
        for (MongoClient client : mongosMongoClients) {
            Document flushRouterConfig = new Document("flushRouterConfig", true);

            try {
                logger.debug(String.format("flushRouterConfig for mongos %s", client.getAddress()));
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
            mongoClient.getDatabase("admin").runCommand(new Document("balancerStop", 1));
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

    public Map<String, ShardCollection> getCollectionsMap() {
        return collectionsMap;
    }

    public MongoClient getMongoClient() {
        return mongoClient;
    }

    public MongoDatabase getConfigDb() {
        return configDb;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public MongoClientOptions getOptions() {
        return mongoClientOptions;
    }

    public List<MongoClient> getMongosMongoClients() {
        return mongosMongoClients;
    }
    
    public MongoClient getShardMongoClient(String shardId) {
        return shardMongoClients.get(shardId);
    }
    
    public Map<String, MongoClient> getShardMongoClients() {
        return shardMongoClients;
    }

    public MongoCredential getCredentials() {
        return mongoClientURI.getCredentials();
    }

    public void checkAutosplit() {
        logger.debug(String.format("checkAutosplit() for %s mongos routers", mongosMongoClients.size()));
        for (MongoClient client : mongosMongoClients) {
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
                    logger.debug("autoSplit disabled for " + client.getAddress());
                } else {
                    logger.warn("autoSplit NOT disabled for " + client.getAddress());
                }
            } catch (MongoTimeoutException timeout) {
                logger.debug("Timeout connecting", timeout);
            }
        }

    }

}
