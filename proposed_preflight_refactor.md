# Proposed Pre-flight Check Refactoring

## Current Implementation
The preflight check is already extracted into `performPreflightCheck()` method in ShardConfigSync.java:1123-1204

## Proposed Enhancement
Further modularize by creating a dedicated `PreflightValidator` class:

```java
public class PreflightValidator {
    private final ShardClient destShardClient;
    private final Logger logger;
    
    public PreflightValidator(ShardClient destShardClient) {
        this.destShardClient = destShardClient;
        this.logger = LoggerFactory.getLogger(PreflightValidator.class);
    }
    
    public PreflightResult validate() {
        PreflightResult result = new PreflightResult();
        
        // Check 1: Database check
        validateDatabases(result);
        
        // Check 2: Sharded collections check
        validateShardedCollections(result);
        
        return result;
    }
    
    private void validateDatabases(PreflightResult result) {
        try {
            destShardClient.preflightCheckEmptyDestination();
        } catch (RuntimeException e) {
            result.addIssue("Database check failed: " + e.getMessage());
        }
    }
    
    private void validateShardedCollections(PreflightResult result) {
        Map<String, RawBsonDocument> destChunksCache = destShardClient.getChunksCache(new BsonDocument());
        
        if (destChunksCache != null && !destChunksCache.isEmpty()) {
            // Count chunks logic...
        }
    }
    
    public static class PreflightResult {
        private final List<String> issues = new ArrayList<>();
        
        public void addIssue(String issue) {
            issues.add(issue);
        }
        
        public boolean isValid() {
            return issues.isEmpty();
        }
        
        public List<String> getIssues() {
            return Collections.unmodifiableList(issues);
        }
    }
}
```

## Benefits
1. Better separation of concerns
2. Easier to test in isolation
3. Reusable across different sync operations
4. More extensible for future validation rules