package com.mongodb.shardsync;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;

/**
 * Integration test that verifies sync metadata ignores destination internal
 * databases prefixed with __mdb_internal.
 */
public class SyncMetadataInternalDbIntegrationTest extends BaseIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(SyncMetadataInternalDbIntegrationTest.class);

    private static final String INTERNAL_DB = "__mdb_internal_whatwhat";
    private static final String INTERNAL_COLL = "stuff";
    private static final String SOURCE_DB = "testSyncMetadataInternalSource";
    private static final String SOURCE_COLL = "normalStuff";

    @BeforeEach
    public void setUp() throws Exception {
        super.baseSetUp();

        dropTestDatabase(INTERNAL_DB);
        dropTestDatabase(SOURCE_DB);

        createShardedCollectionWithDocuments(sourceClient, SOURCE_DB, SOURCE_COLL, 25);
        createShardedCollectionWithDocuments(destClient, INTERNAL_DB, INTERNAL_COLL, 100);
    }

    @AfterEach
    public void tearDown() {
        try {
            dropTestDatabase(SOURCE_DB);
            dropTestDatabase(INTERNAL_DB);
        } catch (Exception e) {
            logger.warn("Error during SyncMetadataInternalDbIntegrationTest cleanup", e);
        }

        super.baseTearDown();
    }

    @Test
    public void testSyncMetadataIgnoresDestinationInternalDatabase() {
        long initialCount = destClient.getDatabase(INTERNAL_DB).getCollection(INTERNAL_COLL).countDocuments();
        assertEquals(100L, initialCount, "Expected destination internal fixture count before sync");

        shardConfigSync.initialize();
        SyncMetadataResult result = shardConfigSync.syncMetadataOptimized(false);

        assertTrue(result.isOverallSuccess(),
                "sync metadata should succeed with destination internal db present. Failures: " + failedSteps(result));

        long finalCount = destClient.getDatabase(INTERNAL_DB).getCollection(INTERNAL_COLL).countDocuments();
        assertEquals(100L, finalCount,
                "Destination internal database collection should remain unchanged after sync metadata");
    }

    private String failedSteps(SyncMetadataResult result) {
        List<SyncMetadataResult.StepResult> failed = new ArrayList<>(result.getFailedSteps());
        if (failed.isEmpty()) {
            return "[]";
        }
        return failed.stream()
                .map(step -> step.getStepName() + ": " + step.getMessage())
                .collect(Collectors.joining("; "));
    }

    private void createShardedCollectionWithDocuments(com.mongodb.client.MongoClient client,
            String databaseName,
            String collectionName,
            int documentCount) {
        client.getDatabase("admin").runCommand(new Document("enableSharding", databaseName));

        String namespace = databaseName + "." + collectionName;
        client.getDatabase("admin").runCommand(
                new Document("shardCollection", namespace).append("key", new Document("_id", "hashed")));

        MongoCollection<Document> collection = client.getDatabase(databaseName).getCollection(collectionName);
        List<Document> documents = new ArrayList<>();
        for (int index = 0; index < documentCount; index++) {
            documents.add(new Document("_id", index).append("value", index));
        }
        collection.insertMany(documents);
    }
}