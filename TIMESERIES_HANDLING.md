# MongoDB Timeseries Collection Handling in ShardConfigSync

## Overview

MongoDB timeseries collections have a dual-collection architecture that requires special handling during shard synchronization. ShardConfigSync includes comprehensive support for timeseries collections, managing both the user-facing view and the underlying bucket collection.

## Timeseries Architecture in MongoDB

### Collection Types
- **View Collection**: `db.metrics` - User-facing collection for queries and inserts
- **Bucket Collection**: `db.system.buckets.metrics` - Internal collection that stores the actual data and is sharded

### Key Characteristics
- Only the **bucket collection** can be sharded directly
- The **view collection** provides the timeseries interface but contains no data
- Chunk operations (splits, moves) must target the **bucket collection**
- UUID operations may require bucket collection lookups when view UUIDs are null

## Implementation Details

### 1. Namespace Conversion Utility (`TimeseriesUtil.java`)

```java
// Check if a collection is a bucket collection
TimeseriesUtil.isBucketCollection("system.buckets.metrics") // true
TimeseriesUtil.isBucketCollection("metrics") // false

// Convert between view and bucket namespaces
TimeseriesUtil.viewToBucketNamespace("db.metrics") // "db.system.buckets.metrics"
TimeseriesUtil.bucketToViewNamespace("db.system.buckets.metrics") // "db.metrics"
```

**Key Methods:**
- `isBucketCollection()` - Identifies bucket collections by the `system.buckets.` prefix
- `isBucketNamespace()` - Checks full namespaces for bucket collections
- Bidirectional conversion between view and bucket namespaces

### 2. Collection Creation (`ShardConfigSync.java:2577-2614`)

```java
// Timeseries collections require special handling during creation
Document timeseriesOptions = (Document) collectionOptions.get("timeseries");
if (timeseriesOptions != null) {
    // Create the timeseries view collection with proper options
    destShardClient.getDatabase(ns.getDatabaseName())
        .createCollection(ns.getCollectionName(), 
            new CreateCollectionOptions().timeSeriesOptions(tsOptions));
}
```

**Process:**
1. Detects timeseries collections by presence of `timeseries` options
2. Extracts `timeField`, `metaField`, and `granularity` settings
3. Creates collection using MongoDB's `TimeSeriesOptions`
4. Automatically generates the corresponding bucket collection

### 3. Sharding Operations (`ShardConfigSync.java:2340-2355`)

```java
// Timeseries collections are identified and handled specially
Document timeseriesFields = (Document) sourceColl.get("timeseriesFields");
if (timeseriesFields != null) {
    boolean success = shardCollection(sourceColl);
    if (success) {
        successfullySharded++;
        logger.debug("Successfully sharded timeseries collection: {}", nsStr);
    }
}
```

**Key Points:**
- Uses `timeseriesFields` metadata to identify timeseries collections
- Sharding targets the bucket collection automatically
- Special logging distinguishes timeseries from regular collections

### 4. Chunk Management (`ChunkManager.java`)

**Critical Fix for splitAt Operations:**
```java
// BEFORE (incorrect):
if (targetNs.contains(".system.buckets.")) {
    targetNs = targetNs.replace(".system.buckets.", ".");
    // This incorrectly converted bucket ns to view ns
}

// AFTER (correct):
String targetNs = mega2.getNs(); // Use bucket namespace directly
Document splitResult = destShardClient.splitAt(targetNs, mid, true);
```

**Why This Matters:**
- Chunk splits must target the **bucket collection** (the actual sharded collection)
- Previous code incorrectly converted to view namespace, causing "NamespaceNotSharded" errors
- Fixed implementation ensures splitAt operations work correctly for timeseries

### 5. UUID Handling (`ShardConfigSync.java:2099-2113`)

```java
// Special handling for timeseries UUID lookups
if (uuid == null && TimeseriesUtil.isBucketNamespace(namespace)) {
    // For bucket collections, try to get UUID from the view collection
    String viewNamespace = TimeseriesUtil.bucketToViewNamespace(namespace);
    Document viewCollection = allCollections.get(viewCollectionName);
    if (viewCollection != null) {
        uuid = viewCollection.get("info", Document.class).getString("uuid");
    }
}
```

**Rationale:**
- Timeseries view collections sometimes have null UUIDs
- Bucket collections may need UUID lookup from their corresponding view
- Ensures UUID comparison operations work correctly

### 6. Collection Filtering

```java
// Skip bucket collections during certain operations
if (TimeseriesUtil.isBucketCollection(ns.getCollectionName())) {
    continue; // Skip bucket collections - work with views instead
}
```

**Applied In:**
- Collection listing and counting
- Shard command operations  
- Metadata comparison operations

## Error Handling

### Common Issues and Solutions

1. **"NamespaceNotSharded" Errors**
   - **Cause**: Attempting to split chunks on view collection instead of bucket collection
   - **Solution**: splitAt operations now target bucket collections directly

2. **Missing UUID Errors**
   - **Cause**: Timeseries view collections with null UUIDs
   - **Solution**: Fallback UUID lookup from bucket collection

3. **Collection Count Mismatches**
   - **Cause**: Including bucket collections in shardable collection counts
   - **Solution**: Filter out bucket collections when counting shardable collections

## Best Practices

### 1. Namespace Handling
- Always use `TimeseriesUtil` for namespace conversions
- Be explicit about whether you're working with view or bucket collections
- Validate namespace types before operations

### 2. Chunk Operations
- Direct all chunk splits to bucket collections
- Never attempt chunk operations on view collections
- Use bucket collection names in chunk queries

### 3. Metadata Operations
- Handle null UUIDs gracefully for timeseries collections
- Consider both view and bucket collections during comparisons
- Filter bucket collections from user-facing collection counts

## Code Examples

### Creating a Timeseries Collection
```java
TimeSeriesOptions tsOptions = new TimeSeriesOptions("timestamp")
    .metaField("metadata")
    .granularity(TimeSeriesGranularity.HOURS);

database.createCollection("metrics", 
    new CreateCollectionOptions().timeSeriesOptions(tsOptions));
// Creates: metrics (view) + system.buckets.metrics (bucket)
```

### Sharding a Timeseries Collection
```java
// Shard the view collection - MongoDB handles bucket collection automatically
db.runCommand({
    shardCollection: "mydb.metrics",
    key: { "metadata.sensorId": 1, "timestamp": 1 }
});
```

### Splitting Chunks
```java
// Always target the bucket collection for chunk operations
String bucketNamespace = "mydb.system.buckets.metrics";
Document splitResult = shardClient.splitAt(bucketNamespace, splitPoint, true);
```

## Implementation Files

- **`TimeseriesUtil.java`** - Namespace conversion utilities
- **`ShardConfigSync.java`** - Main sync logic with timeseries support
- **`ChunkManager.java`** - Chunk operations for timeseries collections
- **`ShardClient.java`** - Low-level operations with timeseries awareness

## Conclusion

The ShardConfigSync timeseries implementation provides:
- **Transparent handling** of dual-collection architecture
- **Robust error handling** for edge cases
- **Correct namespace routing** for different operation types
- **Comprehensive metadata support** for timeseries-specific features

This ensures that timeseries collections are synchronized correctly while maintaining MongoDB's timeseries semantics and performance characteristics.