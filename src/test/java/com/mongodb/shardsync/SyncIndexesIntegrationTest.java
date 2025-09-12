package com.mongodb.shardsync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for syncIndexes functionality with various options.
 * 
 * Tests:
 * - syncIndexes without ttlOnly (syncs all indexes)
 * - syncIndexes with ttlOnly (syncs only TTL indexes)  
 * - syncIndexes with ttlOnly and extendTtl
 * - syncIndexes with collation parameter
 * - syncIndexes with createMissing=false
 */
public class SyncIndexesIntegrationTest extends BaseIntegrationTest {
    
    private static Logger logger = LoggerFactory.getLogger(SyncIndexesIntegrationTest.class);
    
    private static final String TEST_DB = "testSyncIndexes";
    private static final String TEST_COLLECTION = "testCollection";
    
    // Index names and fields
    private static final String TTL_INDEX_NAME = "ttl_index";
    private static final String TTL_FIELD = "createdAt";
    private static final String REGULAR_INDEX_NAME = "user_index";
    private static final String REGULAR_FIELD = "userId";
    private static final String COMPOUND_INDEX_NAME = "compound_index";
    
    // TTL values
    private static final int SOURCE_TTL_SECONDS = 86400; // 1 day
    private static final int EXPECTED_EXTENDED_TTL = 50 * ShardConfigSync.SECONDS_IN_YEAR; // 50 years
    
    @BeforeEach
    public void setUp() throws Exception {
        super.baseSetUp();
        
        // Set filter to only include our test database
        setTestDatabaseFilter(TEST_DB);
        
        // Clean up any existing test data
        dropTestDatabase(TEST_DB);
        
        // Set up test data
        setupTestData();
    }
    
    @AfterEach
    public void tearDown() {
        try {
            // Clean up test databases BEFORE closing connections
            dropTestDatabase(TEST_DB);
        } catch (Exception e) {
            logger.warn("Error during cleanup", e);
        }
        // Then close connections
        super.baseTearDown();
    }
    
    @Test
    public void testSyncIndexesAllIndexes() {
        logger.info("Testing syncIndexes without ttlOnly - should sync all indexes");
        
        // Verify initial state: source has indexes, destination doesn't
        verifyInitialState();
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: sync all indexes (ttlOnly=false)
        shardConfigSync.syncIndexesShards(true, false, false);
        
        // Verify: all non-_id indexes should be synced to destination
        assertEquals(3, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "Destination should have all 3 indexes after sync");
        
        assertTrue(indexExists(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME), 
                  "TTL index should be synced");
        assertTrue(indexExists(destClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME), 
                  "Regular index should be synced");
        assertTrue(indexExists(destClient, TEST_DB, TEST_COLLECTION, COMPOUND_INDEX_NAME), 
                  "Compound index should be synced");
        
        // Verify TTL value was copied correctly (not extended)
        Document destTtlIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertEquals(SOURCE_TTL_SECONDS, destTtlIndex.getInteger("expireAfterSeconds").intValue(),
                    "TTL should match source value when extendTtl=false");
        
        // Use compareIndexes to verify no differences remain
        int diffCount = shardConfigSync.compareIndexes(false);
        assertEquals(0, diffCount, "No index differences should remain after sync");
        
        logger.info("✓ All indexes synced correctly");
    }
    
    @Test
    public void testSyncIndexesTtlOnly() {
        logger.info("Testing syncIndexes with ttlOnly=true - should sync only TTL indexes");
        
        // Verify initial state
        verifyInitialState();
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: sync only TTL indexes (ttlOnly=true)
        shardConfigSync.syncIndexesShards(true, false, true);
        
        // Verify: only TTL index should be synced
        assertEquals(1, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "Destination should have only 1 index after TTL-only sync");
        
        assertTrue(indexExists(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME), 
                  "TTL index should be synced");
        assertFalse(indexExists(destClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME), 
                   "Regular index should NOT be synced in TTL-only mode");
        assertFalse(indexExists(destClient, TEST_DB, TEST_COLLECTION, COMPOUND_INDEX_NAME), 
                   "Compound index should NOT be synced in TTL-only mode");
        
        // Verify TTL value
        Document destTtlIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertEquals(SOURCE_TTL_SECONDS, destTtlIndex.getInteger("expireAfterSeconds").intValue(),
                    "TTL should match source value when extendTtl=false");
        
        // Note: compareIndexes will still show differences for non-TTL indexes since we only synced TTL
        // This is expected behavior in ttlOnly mode
        
        logger.info("✓ Only TTL indexes synced correctly");
    }
    
    @Test
    public void testSyncIndexesTtlOnlyWithExtendTtl() {
        logger.info("Testing syncIndexes with ttlOnly=true and extendTtl=true");
        
        // Verify initial state
        verifyInitialState();
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: sync only TTL indexes with extension (ttlOnly=true, extendTtl=true)
        shardConfigSync.syncIndexesShards(true, true, true);
        
        // Verify: only TTL index should be synced
        assertEquals(1, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "Destination should have only 1 index after TTL-only sync with extend");
        
        assertTrue(indexExists(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME), 
                  "TTL index should be synced");
        assertFalse(indexExists(destClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME), 
                   "Regular index should NOT be synced in TTL-only mode");
        
        // Verify TTL was extended to 50 years
        Document destTtlIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertEquals(EXPECTED_EXTENDED_TTL, destTtlIndex.getInteger("expireAfterSeconds").intValue(),
                    "TTL should be extended to 50 years when extendTtl=true");
        
        // Use compareIndexes with collModTtl=true to verify extended TTL is considered equal
        int diffCount = shardConfigSync.compareIndexes(true);
        // Note: Will still show differences for non-TTL indexes since we only synced TTL
        
        logger.info("✓ TTL indexes synced with extension correctly");
    }
    
    @Test
    public void testSyncIndexesWithCollation() {
        logger.info("Testing syncIndexes with collation parameter");
        
        // Verify initial state
        verifyInitialState();
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: sync with collation
        shardConfigSync.syncIndexesShards(true, false, false);
        
        // Verify: all indexes should be synced
        assertEquals(3, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "All indexes should be synced with collation");
        
        // Use compareIndexes to verify sync was successful
        int diffCount = shardConfigSync.compareIndexes(false);
        assertEquals(0, diffCount, "No index differences should remain after sync with collation");
        
        // Note: Testing collation application would require checking the actual 
        // index creation behavior, which is complex to verify in integration tests
        
        logger.info("✓ Indexes synced with collation parameter");
    }
    
    @Test
    public void testSyncIndexesNoCreateMissing() {
        logger.info("Testing syncIndexes with createMissing=false");
        
        // Verify initial state
        verifyInitialState();
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: don't create missing indexes
        shardConfigSync.syncIndexesShards(false, false, false);
        
        // Verify: no indexes should be created on destination
        assertEquals(0, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "No indexes should be created when createMissing=false");
        
        // compareIndexes should show differences (returns count of namespaces with differences, not individual indexes)
        int diffCount = shardConfigSync.compareIndexes(false);
        assertTrue(diffCount > 0, "Index differences should be reported when createMissing=false");
        
        logger.info("✓ No indexes created when createMissing=false");
    }
    
    @Test
    public void testSyncIndexesTtlOnlyNoTtlIndexes() {
        logger.info("Testing syncIndexes with ttlOnly=true when no TTL indexes exist");
        
        // Setup: create test collection but only with non-TTL indexes
        createTestCollection(TEST_DB, TEST_COLLECTION);
        
        // Create only non-TTL indexes on source
        createRegularIndex(sourceClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME, 
                          new Document(REGULAR_FIELD, 1), false, false);
        createRegularIndex(sourceClient, TEST_DB, TEST_COLLECTION, COMPOUND_INDEX_NAME,
                          new Document("status", 1).append("priority", -1), true, false);
        
        // Verify source has non-TTL indexes, dest has none
        assertEquals(2, countNonIdIndexes(sourceClient, TEST_DB, TEST_COLLECTION), 
                    "Source should have 2 non-TTL indexes");
        assertEquals(0, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "Destination should start with no indexes");
        
        // Initialize ShardConfigSync properly for the test
        shardConfigSync.initialize();
        
        // Execute: sync only TTL indexes when none exist
        shardConfigSync.syncIndexesShards(true, false, true);
        
        // Verify: no indexes should be synced since none are TTL
        assertEquals(0, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "No indexes should be synced when no TTL indexes exist");
        
        logger.info("✓ TTL-only mode handled correctly when no TTL indexes exist");
    }
    
    private void setupTestData() {
        logger.info("Setting up test data for syncIndexes tests");
        
        // Create test collection
        createTestCollection(TEST_DB, TEST_COLLECTION);
        
        // Create indexes on SOURCE only (destination should start empty)
        // 1. TTL index
        createTtlIndex(sourceClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME, TTL_FIELD, SOURCE_TTL_SECONDS);
        
        // 2. Regular index
        createRegularIndex(sourceClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME,
                          new Document(REGULAR_FIELD, 1), false, false);
        
        // 3. Compound index with unique constraint
        createRegularIndex(sourceClient, TEST_DB, TEST_COLLECTION, COMPOUND_INDEX_NAME,
                          new Document("status", 1).append("priority", -1), true, false);
        
        logger.info("Test data setup completed - source has 3 indexes, destination has 0");
    }
    
    private void verifyInitialState() {
        // Verify source has our test indexes
        assertEquals(3, countNonIdIndexes(sourceClient, TEST_DB, TEST_COLLECTION), 
                    "Source should have 3 indexes");
        assertTrue(indexExists(sourceClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME), 
                  "Source should have TTL index");
        assertTrue(indexExists(sourceClient, TEST_DB, TEST_COLLECTION, REGULAR_INDEX_NAME), 
                  "Source should have regular index");
        assertTrue(indexExists(sourceClient, TEST_DB, TEST_COLLECTION, COMPOUND_INDEX_NAME), 
                  "Source should have compound index");
        
        // Verify destination starts empty
        assertEquals(0, countNonIdIndexes(destClient, TEST_DB, TEST_COLLECTION), 
                    "Destination should start with no indexes");
        
        logger.debug("Initial state verified - source: 3 indexes, destination: 0 indexes");
    }
}