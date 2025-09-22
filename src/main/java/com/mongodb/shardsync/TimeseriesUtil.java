package com.mongodb.shardsync;

/**
 * Utility class for handling timeseries collection namespaces.
 * In MongoDB, timeseries collections have both a view (e.g., "db.metrics") 
 * and an underlying bucket collection (e.g., "db.system.buckets.metrics").
 */
public class TimeseriesUtil {
    
    private static final String SYSTEM_BUCKETS_PREFIX = "system.buckets.";
    
    /**
     * Check if a collection name is a timeseries bucket collection.
     * @param collectionName The collection name to check
     * @return true if it's a bucket collection
     */
    public static boolean isBucketCollection(String collectionName) {
        return collectionName != null && collectionName.startsWith(SYSTEM_BUCKETS_PREFIX);
    }
    
    /**
     * Check if a namespace is a timeseries bucket collection.
     * @param namespace The full namespace (db.collection)
     * @return true if it's a bucket collection
     */
    public static boolean isBucketNamespace(String namespace) {
        if (namespace == null) return false;
        String[] parts = namespace.split("\\.", 2);
        return parts.length == 2 && isBucketCollection(parts[1]);
    }
    
    /**
     * Convert a bucket collection name to its view collection name.
     * @param bucketCollectionName The bucket collection name (e.g., "system.buckets.metrics")
     * @return The view collection name (e.g., "metrics")
     * @throws IllegalArgumentException if bucketCollectionName is null or empty
     */
    public static String bucketToViewCollectionName(String bucketCollectionName) {
        if (bucketCollectionName == null || bucketCollectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (!isBucketCollection(bucketCollectionName)) {
            return bucketCollectionName;
        }
        return bucketCollectionName.substring(SYSTEM_BUCKETS_PREFIX.length());
    }
    
    /**
     * Convert a view collection name to its bucket collection name.
     * @param viewCollectionName The view collection name (e.g., "metrics")
     * @return The bucket collection name (e.g., "system.buckets.metrics")
     * @throws IllegalArgumentException if viewCollectionName is null or empty
     */
    public static String viewToBucketCollectionName(String viewCollectionName) {
        if (viewCollectionName == null || viewCollectionName.isEmpty()) {
            throw new IllegalArgumentException("Collection name cannot be null or empty");
        }
        if (isBucketCollection(viewCollectionName)) {
            return viewCollectionName;
        }
        return SYSTEM_BUCKETS_PREFIX + viewCollectionName;
    }
    
    /**
     * Convert a bucket namespace to its view namespace.
     * @param bucketNamespace The bucket namespace (e.g., "db.system.buckets.metrics")
     * @return The view namespace (e.g., "db.metrics")
     * @throws IllegalArgumentException if bucketNamespace is null, empty, or malformed
     */
    public static String bucketToViewNamespace(String bucketNamespace) {
        if (bucketNamespace == null || bucketNamespace.isEmpty()) {
            throw new IllegalArgumentException("Namespace cannot be null or empty");
        }
        if (!isBucketNamespace(bucketNamespace)) {
            return bucketNamespace;
        }
        String[] parts = bucketNamespace.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid namespace format: " + bucketNamespace);
        }
        return parts[0] + "." + bucketToViewCollectionName(parts[1]);
    }
    
    /**
     * Convert a view namespace to its bucket namespace.
     * @param viewNamespace The view namespace (e.g., "db.metrics")
     * @return The bucket namespace (e.g., "db.system.buckets.metrics")
     * @throws IllegalArgumentException if viewNamespace is null, empty, or malformed
     */
    public static String viewToBucketNamespace(String viewNamespace) {
        if (viewNamespace == null || viewNamespace.isEmpty()) {
            throw new IllegalArgumentException("Namespace cannot be null or empty");
        }
        if (isBucketNamespace(viewNamespace)) {
            return viewNamespace;
        }
        String[] parts = viewNamespace.split("\\.", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid namespace format: " + viewNamespace);
        }
        return parts[0] + "." + viewToBucketCollectionName(parts[1]);
    }
}