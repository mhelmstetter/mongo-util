# Compound Shard Key Balancer Migration Plan

## Overview
This document outlines the changes needed to recreate the compound shard key balancer functionality from branches `compoundShardKeyBalancer`, `compoundShardKeyBalancer2`, and `compoundShardKeyBalancer3` on the current master branch.

## Key Features to Migrate

### 1. **TTL Index Fix (Critical)**
**Source**: compoundShardKeyBalancer3 commit `4fbc315b`
**Location**: `Balancer.java:153`
**Change**: 
```java
// OLD (causes IndexOptionsConflict):
.expireAfter(XXX, TimeUnit.SECONDS)

// NEW (matches existing index):
.expireAfter(43200L, TimeUnit.SECONDS)
```
**Reason**: Prevents IndexOptionsConflict errors when balancer tries to create TTL index that doesn't match existing one.

### 2. **Pre-emptive Chunk Splitting**
**Source**: compoundShardKeyBalancer3
**Location**: `Balancer.java` move chunk logic
**Description**: Check chunk size before move and split if too large
**Implementation**:
```java
// Before moveChunk() call:
Document dataSize = sourceShardClient.dataSize(ns, min, max);
Number countNumber = dataSize.get("numObjects", Number.class);
long count = countNumber.longValue();

while (count >= maxDocs) {
    logger.debug("maxDocs: {}, chunk too big, splitting", maxDocs);
    splitChunk();
    // Recalculate size
    dataSize = sourceShardClient.dataSize(ns, min, max);
    count = dataSize.get("numObjects", Number.class).longValue();
}
```

### 3. **Compound Shard Key Support**
**Core Components**:

#### A. **BsonValueWrapper Enhancement**
**Source**: All three branches
**Location**: `com.mongodb.util.bson.BsonValueWrapper`
**Purpose**: Handle comparison of compound shard keys (BsonDocument) and various BSON types
**Key Features**:
- Support for BsonDocument comparison (compound keys)
- Handle BsonMinKey, BsonMaxKey, BsonBinary, etc.
- Proper sorting for NavigableMap operations

#### B. **Chunk Map Structure**
**Current master**: `Map<String, Map<String, RawBsonDocument>>`
**Branch structure**: `Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>>`
**Purpose**: Enable efficient shard key lookups for compound keys

#### C. **Shard Key Extraction Logic**
**Location**: `TailingOplogAnalyzerWorker.java`
**Purpose**: Extract compound shard keys from oplog operations
**Functions needed**:
- `getShardKeyForOperation()` - Extract shard key from oplog entry
- `getShardKeyFromOplogEntry()` - Handle different oplog operation types
- Support for multi-field shard keys

### 4. **dataSize() Method**
**Source**: compoundShardKeyBalancer3
**Location**: `ShardClient.java`
**New method**:
```java
public Document dataSize(String namespace, BsonDocument min, BsonDocument max) {
    Document cmd = new Document("dataSize", namespace);
    cmd.append("min", min);
    cmd.append("max", max);
    cmd.append("estimate", true);
    Document result = null;
    try {
        result = adminCommand(cmd);
    } catch (MongoCommandException mce) {
        logger.warn("dataSize error ns: {}, message: {}", namespace, mce.getMessage());
    }
    return result;
}
```

### 5. **Enhanced Move Retry Logic**
**Source**: compoundShardKeyBalancer3
**Features**:
- `moveChunkWithRetry()` method with regex parsing of ChunkTooBig errors
- Extract maxDocs from error message: `"maximum number of documents for a chunk is (\\d+)"`
- Automatic chunk splitting when ChunkTooBig occurs

### 6. **Chunk Loading Improvements**
**Source**: All branches
**Features**:
- Support for compound shard key chunk mapping
- Namespace-specific filtering support
- Enhanced chunk cache management

### 7. **OplogUtil Integration**
**Current master has**: `OplogUtil.getIdForOperation(doc, shardKey, shardId)`
**Branches have**: Custom `getShardKeyForOperation()` methods
**Decision**: Use master's OplogUtil but ensure it supports compound keys

## Implementation Plan

### Phase 1: Infrastructure (New Branch Creation)
1. Create new branch `compound-shard-key-v2` from current master
2. Ensure build passes with existing functionality

### Phase 2: Core Support Classes
1. **Update BsonValueWrapper** - Port compound key comparison logic
2. **Add dataSize() method** to ShardClient
3. **Create ComparableBsonDocument** if needed (from branches)

### Phase 3: Balancer Updates
1. **Apply TTL fix** (immediate - critical for production)
2. **Update Balancer.init()** chunk map structure
3. **Add pre-emptive splitting logic** 
4. **Implement moveChunkWithRetry()** method

### Phase 4: Integration Testing
1. **Test with simple shard key** (ensure no regression)
2. **Test with compound shard key** (new functionality)
3. **Test ChunkTooBig scenarios**
4. **Verify TTL index compatibility**

## Key Diffs to Reference

### Critical TTL Fix
```bash
git diff master compoundShardKeyBalancer3 -- src/main/java/com/mongodb/shardbalancer/Balancer.java | grep -A5 -B5 "expireAfter"
```

### BsonValueWrapper Changes
```bash
git diff master compoundShardKeyBalancer3 -- src/main/java/com/mongodb/util/bson/BsonValueWrapper.java
```

### Shard Key Extraction Logic
```bash
git diff master compoundShardKeyBalancer3 -- src/main/java/com/mongodb/shardbalancer/TailingOplogAnalyzerWorker.java
```

## Risk Assessment

**Low Risk**:
- TTL fix (isolated change)
- dataSize() method addition
- BsonValueWrapper enhancements

**Medium Risk**:
- Chunk map structure changes (affects core balancing logic)
- OplogUtil integration (verify compound key support)

**High Risk**:
- Pre-emptive splitting (complex logic, affects move operations)
- Complete replacement of simple shard key logic

## Recommended Approach

1. **Start with TTL fix** - immediate production benefit
2. **Implement dataSize() method** - enables pre-emptive splitting  
3. **Enhance BsonValueWrapper** - foundation for compound keys
4. **Update balancer incrementally** - maintain backward compatibility
5. **Add compound key support** - new functionality on solid foundation

## Validation Criteria

- [ ] Existing simple shard key balancing continues to work
- [ ] TTL indexes don't cause conflicts
- [ ] Compound shard keys can be balanced
- [ ] ChunkTooBig errors are handled gracefully
- [ ] Performance is acceptable for large chunk maps
- [ ] Memory usage is reasonable for NavigableMap structures

## Files to Create/Modify

**New files**:
- None (all functionality exists in branches)

**Modified files**:
- `src/main/java/com/mongodb/shardbalancer/Balancer.java`
- `src/main/java/com/mongodb/shardsync/ShardClient.java` 
- `src/main/java/com/mongodb/util/bson/BsonValueWrapper.java`
- `src/main/java/com/mongodb/shardbalancer/TailingOplogAnalyzerWorker.java` (if needed)
- `src/main/java/com/mongodb/util/bson/ComparableBsonDocument.java` (if needed)

This migration plan preserves the valuable functionality while working with master's improved architecture.