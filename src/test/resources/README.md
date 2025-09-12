# Integration Test Setup

This directory contains configuration and setup instructions for running integration tests.

## Prerequisites

- Two MongoDB instances/clusters for testing:
  - **Source cluster**: The cluster to sync FROM
  - **Destination cluster**: The cluster to sync TO
- Both clusters should be accessible from your test environment
- Clusters should be empty or contain only test data (tests will modify data)

## Configuration Setup

### 1. Create Test Configuration File

Copy the example configuration file and customize it for your environment:

```bash
cp src/test/resources/shard-sync.properties.example src/test/resources/shard-sync.properties
```

### 2. Configure Connection Strings

Edit `src/test/resources/shard-sync.properties` and set:

```properties
# For local MongoDB instances
source=mongodb://localhost:27017/
dest=mongodb://localhost:27018/

# For Atlas or remote clusters  
source=mongodb+srv://username:password@source-cluster.mongodb.net/
dest=mongodb+srv://username:password@dest-cluster.mongodb.net/
```

### 3. Test Database Isolation (Automatic)

Tests automatically filter operations to only affect their specific test databases:
- `testSyncIndexes` database for sync-related tests  
- `testCompareIndexes` database for comparison tests
- No additional configuration needed - this is handled programmatically

## Running Integration Tests

### Run All Integration Tests
```bash
mvn test -Dtest="*IntegrationTest"
```

### Run Specific Test Classes
```bash
# Test compare indexes functionality
mvn test -Dtest="CompareIndexesIntegrationTest"

# Test sync indexes functionality  
mvn test -Dtest="SyncIndexesIntegrationTest"
```

### Run Individual Test Methods
```bash
mvn test -Dtest="CompareIndexesIntegrationTest#testIdenticalIndexes"
```

## MongoDB Setup Examples

### Local Development with Docker

```bash
# Start two MongoDB instances
docker run -d --name mongo-source -p 27017:27017 mongo:7.0
docker run -d --name mongo-dest -p 27018:27017 mongo:7.0

# Configure properties
sourceUri=mongodb://localhost:27017/
destUri=mongodb://localhost:27018/
```

### Local Sharded Clusters

If testing with sharded clusters, ensure both source and destination have:
- Config servers running
- Multiple shard replica sets  
- mongos routers accessible

```properties
# Point to mongos routers
source=mongodb://localhost:27017/
dest=mongodb://localhost:27019/
```

## Test Safety

⚠️ **Important Safety Notes:**

1. **Never run tests against production clusters**
2. **Tests will create/modify/delete data** - use dedicated test environments
3. **Destination cluster will be modified** - ensure it contains no important data
4. **Use database filters** to limit test scope
5. **The properties file is gitignored** - credentials won't be committed

## Troubleshooting

### Connection Issues
- Verify MongoDB instances are running and accessible
- Check connection strings for typos
- Ensure network connectivity (firewalls, VPNs, etc.)
- For Atlas: Verify IP whitelist settings

### Test Failures  
- Check MongoDB logs for errors
- Verify both clusters are empty before running tests
- Ensure proper permissions (read/write access to all databases)
- For sharded clusters: Verify cluster is properly initialized

### Configuration Issues
- Verify `shard-sync.properties` exists in `src/test/resources/`
- Check property syntax (no extra spaces, proper escaping)
- Review test logs for configuration errors

## Test Data Cleanup

Tests should clean up after themselves, but if needed:

```bash
# Connect to test MongoDB instances and drop test databases
mongo localhost:27017
> use testSyncIndexes
> db.dropDatabase()
> use testCompareIndexes  
> db.dropDatabase()
```