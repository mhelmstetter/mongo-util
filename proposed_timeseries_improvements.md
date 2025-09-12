# Proposed Timeseries Collection Handling Improvements

## Current Implementation
Timeseries bucket namespace conversions are scattered across multiple locations:
- ChunkManager.java:534-538, 616-620
- ShardClient.java:1968-1973
- ShardConfigSync.java:219-223, 1705-1710, 1740-1745, 1973-1978

## Proposed Solution: Centralize Namespace Conversion Logic

### Create a Dedicated Utility Class
```java
public class TimeseriesNamespaceUtil {
    private static final String BUCKET_PREFIX = "system.buckets.";
    private static final Logger logger = LoggerFactory.getLogger(TimeseriesNamespaceUtil.class);
    
    /**
     * Checks if a namespace represents a timeseries bucket collection
     */
    public static boolean isBucketNamespace(String namespace) {
        if (namespace == null) return false;
        int dotIndex = namespace.indexOf('.');
        if (dotIndex == -1) return false;
        
        String collectionName = namespace.substring(dotIndex + 1);
        return collectionName.startsWith(BUCKET_PREFIX);
    }
    
    /**
     * Converts a bucket namespace to its corresponding view namespace
     * @param bucketNamespace The bucket namespace (e.g., "db.system.buckets.metrics")
     * @return The view namespace (e.g., "db.metrics")
     * @throws IllegalArgumentException if not a bucket namespace
     */
    public static String toViewNamespace(String bucketNamespace) {
        if (!isBucketNamespace(bucketNamespace)) {
            throw new IllegalArgumentException("Not a bucket namespace: " + bucketNamespace);
        }
        
        int dotIndex = bucketNamespace.indexOf('.');
        String dbName = bucketNamespace.substring(0, dotIndex);
        String collectionName = bucketNamespace.substring(dotIndex + 1);
        String viewName = collectionName.substring(BUCKET_PREFIX.length());
        
        String viewNamespace = dbName + "." + viewName;
        logger.debug("Converting bucket namespace {} to view namespace {}", 
                    bucketNamespace, viewNamespace);
        return viewNamespace;
    }
    
    /**
     * Converts a view namespace to its corresponding bucket namespace
     * @param viewNamespace The view namespace (e.g., "db.metrics")
     * @return The bucket namespace (e.g., "db.system.buckets.metrics")
     */
    public static String toBucketNamespace(String viewNamespace) {
        int dotIndex = viewNamespace.indexOf('.');
        if (dotIndex == -1) {
            throw new IllegalArgumentException("Invalid namespace: " + viewNamespace);
        }
        
        String dbName = viewNamespace.substring(0, dotIndex);
        String viewName = viewNamespace.substring(dotIndex + 1);
        return dbName + "." + BUCKET_PREFIX + viewName;
    }
    
    /**
     * Gets the appropriate namespace for operations based on context
     */
    public static String getOperationalNamespace(String namespace, OperationContext context) {
        if (!isBucketNamespace(namespace)) {
            return namespace;
        }
        
        switch (context) {
            case SPLIT_OPERATION:
            case SHARD_COLLECTION:
            case CONFIG_ENTRY:
                return toViewNamespace(namespace);
            case CHUNK_STORAGE:
            case INTERNAL_OPERATION:
                return namespace;
            default:
                return namespace;
        }
    }
    
    public enum OperationContext {
        SPLIT_OPERATION,
        SHARD_COLLECTION,
        CONFIG_ENTRY,
        CHUNK_STORAGE,
        INTERNAL_OPERATION
    }
}
```

### Update Existing Code to Use Utility
```java
// Before (in ChunkManager.java):
if (targetNs.contains(".system.buckets.")) {
    targetNs = targetNs.replace(".system.buckets.", ".");
    logger.debug("Converting bucket namespace {} to view namespace {} for split operation", 
                 mega2.getNs(), targetNs);
}

// After:
if (TimeseriesNamespaceUtil.isBucketNamespace(targetNs)) {
    targetNs = TimeseriesNamespaceUtil.toViewNamespace(targetNs);
}
```

## Benefits
1. Single source of truth for timeseries namespace logic
2. Consistent handling across the codebase
3. Easier to test and maintain
4. Better error handling with explicit validation
5. Clear documentation of when conversions are needed