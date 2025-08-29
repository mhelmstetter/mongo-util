package com.mongodb.shardsync;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

/**
 * Base class for integration tests that provides common MongoDB setup and utilities.
 * 
 * This class handles:
 * - Loading test configuration from shard-sync.properties
 * - Setting up MongoDB clients for source and destination
 * - Providing utility methods for index and collection management
 * - Test cleanup
 */
public abstract class BaseIntegrationTest {
    
    protected static Logger logger = LoggerFactory.getLogger(BaseIntegrationTest.class);
    
    protected MongoClient sourceClient;
    protected MongoClient destClient;
    protected SyncConfiguration config;
    protected ShardConfigSync shardConfigSync;
    
    @BeforeEach
    public void baseSetUp() throws IOException {
        // Load configuration from test properties file
        config = loadConfiguration();
        
        // Initialize MongoDB clients
        sourceClient = MongoClients.create(config.getSourceClusterUri());
        destClient = MongoClients.create(config.getDestClusterUri());
        
        // Initialize ShardConfigSync
        shardConfigSync = new ShardConfigSync(config);
        
        // Set filter to only include test databases (to avoid interference from other databases)
        // This will be applied to each test's specific database
        // Individual tests should call setTestDatabaseFilter() with their database name
        
        logger.info("Base setup completed - source: {}, dest: {}", 
                   maskUri(config.getSourceClusterUri()), maskUri(config.getDestClusterUri()));
    }
    
    @AfterEach
    public void baseTearDown() {
        try {
            if (sourceClient != null) {
                sourceClient.close();
            }
            if (destClient != null) {
                destClient.close();
            }
        } catch (Exception e) {
            logger.warn("Error during base cleanup", e);
        }
    }
    
    /**
     * Load test configuration from shard-sync.properties in test resources
     */
    protected SyncConfiguration loadConfiguration() throws IOException {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE)) {
            if (input == null) {
                throw new IOException("Unable to find " + ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE + " in test resources");
            }
            props.load(input);
        }
        
        SyncConfiguration config = new SyncConfiguration();
        config.setSourceClusterUri(props.getProperty("source"));
        config.setDestClusterUri(props.getProperty("dest"));
        
        // Verify we got the properties  
        if (config.getSourceClusterUri() == null || config.getDestClusterUri() == null) {
            throw new IOException("Missing required properties: source=" + config.getSourceClusterUri() + ", dest=" + config.getDestClusterUri());
        }
        
        // Set default values for required fields
        config.nonPrivilegedMode = true;
        config.extendTtl = false; // Override in individual tests as needed
        
        return config;
    }
    
    /**
     * Create a test database and collection on both source and destination
     */
    protected void createTestCollection(String dbName, String collectionName) {
        logger.debug("Creating test collection: {}.{}", dbName, collectionName);
        
        MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
        MongoDatabase destDb = destClient.getDatabase(dbName);
        
        // Drop existing collections if they exist
        try {
            sourceDb.getCollection(collectionName).drop();
            destDb.getCollection(collectionName).drop();
        } catch (Exception e) {
            // Ignore errors if collections don't exist
        }
        
        // Create collections
        sourceDb.createCollection(collectionName);
        destDb.createCollection(collectionName);
        
        // Insert a test document to ensure collections exist
        Document testDoc = new Document("_id", 1)
                .append("testField", "testValue")
                .append("createdAt", new java.util.Date());
        
        sourceDb.getCollection(collectionName).insertOne(testDoc);
        destDb.getCollection(collectionName).insertOne(testDoc);
    }
    
    /**
     * Create a TTL index on the specified collection
     */
    protected void createTtlIndex(MongoClient client, String dbName, String collectionName, 
                                 String indexName, String fieldName, int ttlSeconds) {
        logger.debug("Creating TTL index: {} on {}.{}.{} with TTL {}s", 
                    indexName, client.toString(), dbName, collectionName, ttlSeconds);
        
        MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
        
        IndexOptions options = new IndexOptions()
                .name(indexName)
                .expireAfter((long) ttlSeconds, java.util.concurrent.TimeUnit.SECONDS);
        
        collection.createIndex(Indexes.ascending(fieldName), options);
    }
    
    /**
     * Create a regular (non-TTL) index on the specified collection
     */
    protected void createRegularIndex(MongoClient client, String dbName, String collectionName,
                                    String indexName, Document keyPattern, boolean unique, boolean sparse) {
        logger.debug("Creating regular index: {} on {}.{}", indexName, dbName, collectionName);
        
        MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
        
        IndexOptions options = new IndexOptions()
                .name(indexName)
                .unique(unique)
                .sparse(sparse);
        
        collection.createIndex(keyPattern, options);
    }
    
    /**
     * Get index information by name
     */
    protected Document getIndexInfo(MongoClient client, String dbName, String collectionName, String indexName) {
        MongoDatabase db = client.getDatabase(dbName);
        MongoCollection<Document> collection = db.getCollection(collectionName);
        
        for (Document index : collection.listIndexes()) {
            if (indexName.equals(index.getString("name"))) {
                return index;
            }
        }
        return null;
    }
    
    /**
     * Drop a test database on both source and destination
     */
    protected void dropTestDatabase(String dbName) {
        try {
            if (sourceClient != null) {
                sourceClient.getDatabase(dbName).drop();
            }
            if (destClient != null) {
                destClient.getDatabase(dbName).drop();
            }
            logger.debug("Dropped test database: {}", dbName);
        } catch (Exception e) {
            logger.warn("Error dropping test database: {}", dbName, e);
        }
    }
    
    /**
     * Count the number of indexes on a collection (excluding _id_ index)
     */
    protected int countNonIdIndexes(MongoClient client, String dbName, String collectionName) {
        MongoCollection<Document> collection = client.getDatabase(dbName).getCollection(collectionName);
        int count = 0;
        for (Document index : collection.listIndexes()) {
            String indexName = index.getString("name");
            if (!"_id_".equals(indexName)) {
                count++;
            }
        }
        return count;
    }
    
    /**
     * Check if an index exists on a collection
     */
    protected boolean indexExists(MongoClient client, String dbName, String collectionName, String indexName) {
        return getIndexInfo(client, dbName, collectionName, indexName) != null;
    }
    
    /**
     * Set namespace filter to only include the specified test database
     * This prevents interference from other databases during testing
     */
    protected void setTestDatabaseFilter(String testDbName) {
        config.setNamespaceFilters(new String[]{testDbName});
        logger.debug("Set namespace filter to: {}", testDbName);
    }
    
    /**
     * Mask URI for logging (hide credentials)
     */
    private String maskUri(String uri) {
        if (uri == null) return null;
        // Simple masking - replace anything between :// and @ with ***
        return uri.replaceAll("://[^@]+@", "://***@");
    }
}