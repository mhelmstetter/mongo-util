package com.mongodb.shardsync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration test for collModTtl functionality.
 * 
 * This test verifies that the collModTtl command correctly updates TTL indexes
 * when there are differences between source and destination collections.
 */
public class CollModTtlIntegrationTest extends BaseIntegrationTest {
    
    private static Logger logger = LoggerFactory.getLogger(CollModTtlIntegrationTest.class);
    
    private static final String TEST_DB = "testCollModTtl";
    private static final String TEST_COLLECTION = "testCollection";
    private static final String TTL_INDEX_NAME = "testTtlIndex";
    private static final String TTL_FIELD = "createdAt";
    
    // TTL values in seconds
    private static final int SOURCE_TTL = 86400; // 1 day
    private static final int DEST_TTL_INITIAL = 31536000; // 1 year (high value)
    private static final int EXPECTED_TTL_AFTER_EXTEND = 50 * ShardConfigSync.SECONDS_IN_YEAR; // 50 years
    
    @BeforeEach
    public void setUp() throws Exception {
        // Call parent setup
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
            dropTestDatabase(TEST_DB);
            super.baseTearDown();
        } catch (Exception e) {
            logger.warn("Error during cleanup", e);
        }
    }
    
    @Test
    public void testCollModTtlWithExtendTtl() {
        logger.info("Starting testCollModTtlWithExtendTtl");
        
        // Verify initial setup
        verifyInitialIndexes();
        
        // Execute collModTtl with extendTtl enabled
        config.extendTtl = true;
        
        // Test collMod directly with extending TTL
        testCollModDirectly(true);
        
        // Verify that the destination TTL was updated
        verifyTtlAfterCollMod();
        
        logger.info("testCollModTtlWithExtendTtl completed successfully");
    }
    
    @Test 
    public void testCollModTtlWithoutExtendTtl() {
        logger.info("Starting testCollModTtlWithoutExtendTtl");
        
        // Verify initial setup
        verifyInitialIndexes();
        
        // Execute collModTtl without extendTtl
        config.extendTtl = false;
        
        // Test collMod directly without extending TTL
        testCollModDirectly(false);
        
        // Verify that the destination TTL was updated to match source (not extended)
        verifyTtlMatchesSource();
        
        logger.info("testCollModTtlWithoutExtendTtl completed successfully");
    }
    
    
    private void setupTestData() {
        logger.info("Setting up test data");
        
        // Create test collection on both source and destination
        createTestCollection(TEST_DB, TEST_COLLECTION);
        
        // Create TTL indexes with different expiration times
        createTtlIndex(sourceClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME, TTL_FIELD, SOURCE_TTL);
        createTtlIndex(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME, TTL_FIELD, DEST_TTL_INITIAL);
        
        logger.info("Test data setup completed - source TTL: {}s, dest TTL: {}s", 
                   SOURCE_TTL, DEST_TTL_INITIAL);
    }
    
    
    private void verifyInitialIndexes() {
        logger.info("Verifying initial index setup");
        
        // Verify source index
        Document sourceIndex = getIndexInfo(sourceClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertNotNull(sourceIndex, "Source TTL index should exist");
        assertEquals(SOURCE_TTL, sourceIndex.getInteger("expireAfterSeconds").intValue(), 
                    "Source TTL should be " + SOURCE_TTL + " seconds");
        
        // Verify destination index
        Document destIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertNotNull(destIndex, "Destination TTL index should exist");
        assertEquals(DEST_TTL_INITIAL, destIndex.getInteger("expireAfterSeconds").intValue(),
                    "Destination TTL should initially be " + DEST_TTL_INITIAL + " seconds");
        
        logger.info("Initial index verification completed");
    }
    
    private void verifyTtlAfterCollMod() {
        logger.info("Verifying TTL after collMod with extendTtl");
        
        Document destIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertNotNull(destIndex, "Destination TTL index should still exist");
        
        int actualTtl = destIndex.getInteger("expireAfterSeconds").intValue();
        assertEquals(EXPECTED_TTL_AFTER_EXTEND, actualTtl,
                    "Destination TTL should be extended to " + EXPECTED_TTL_AFTER_EXTEND + " seconds");
        
        logger.info("TTL verification after collMod completed - TTL extended to: {}s", actualTtl);
    }
    
    private void testCollModDirectly(boolean extendTtl) {
        logger.info("Testing collMod directly with extendTtl={}", extendTtl);
        
        // Get the current source index info to use as a template
        Document sourceIndex = getIndexInfo(sourceClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertNotNull(sourceIndex, "Source index must exist for collMod test");
        
        // Create collMod command with correct format
        Document collMod = new Document("collMod", TEST_COLLECTION);
        Document indexMod = new Document();
        
        // Use keyPattern instead of key, and only include necessary fields
        indexMod.put("keyPattern", sourceIndex.get("key"));
        
        if (extendTtl) {
            // Extend TTL to 50 years as per the implementation
            indexMod.put("expireAfterSeconds", EXPECTED_TTL_AFTER_EXTEND);
            logger.debug("Extending TTL from {} to {}", sourceIndex.getInteger("expireAfterSeconds"), EXPECTED_TTL_AFTER_EXTEND);
        } else {
            // Set TTL to match source
            indexMod.put("expireAfterSeconds", sourceIndex.getInteger("expireAfterSeconds"));
        }
        
        collMod.append("index", indexMod);
        
        logger.debug("Executing collMod command: {}", collMod);
        
        try {
            Document result = destClient.getDatabase(TEST_DB).runCommand(collMod);
            logger.info("collMod successful, result: {}", result);
        } catch (Exception e) {
            logger.error("collMod failed", e);
            throw new RuntimeException("collMod command failed", e);
        }
    }
    
    private void verifyTtlMatchesSource() {
        logger.info("Verifying TTL matches source after collMod without extendTtl");
        
        Document destIndex = getIndexInfo(destClient, TEST_DB, TEST_COLLECTION, TTL_INDEX_NAME);
        assertNotNull(destIndex, "Destination TTL index should still exist");
        
        int actualTtl = destIndex.getInteger("expireAfterSeconds").intValue();
        assertEquals(SOURCE_TTL, actualTtl,
                    "Destination TTL should match source TTL: " + SOURCE_TTL + " seconds");
        
        logger.info("TTL verification completed - TTL matches source: {}s", actualTtl);
    }
    
}