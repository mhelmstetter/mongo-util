# CustomDocumentAnalyzer: MongoDB Shard Balancer Architecture

## Overview

The `CustomDocumentAnalyzer` is a sophisticated MongoDB shard balancer designed to optimize chunk distribution in sharded clusters. It operates in three distinct phases: **initialization**, **balancing**, and **migration**. The system focuses on moving large documents (≥1MB BSON size) to achieve better data distribution across shards.

## Core Architecture

### Class Hierarchy
```
Balancer (abstract base class)
└── CustomDocumentAnalyzer (concrete implementation)
```

### Key Components

1. **BalancerConfig**: Configuration management and namespace definitions
2. **ChunkManager**: Handles chunk metadata and chunk map operations  
3. **ShardClient**: MongoDB cluster connection and shard-specific operations
4. **Status Tracking**: Real-time monitoring and periodic reporting

### Dependencies
- **MongoDB Driver**: Core database operations and aggregation
- **Apache Commons**: Configuration management and utilities
- **SLF4J/Logback**: Comprehensive logging with status reporting
- **PicoCLI**: Command-line interface and argument parsing

## Operational Modes

### 1. Initialization Mode (`--init`)

**Purpose**: Analyze documents on a specific shard and build metadata for balancing decisions.

**Workflow**:
1. **Target Selection**: User specifies shard index and year-month pattern (YYYYMM)
2. **Document Discovery**: Queries documents with `_id` matching pattern `.*:YYYYMM$`
3. **Chunk Mapping**: Maps each document to its containing chunk using chunk boundaries
4. **Metadata Creation**: Stores document statistics in `mongoCustomBalancerStats.chunkStats`:
   ```javascript
   {
     _id: document._id,
     bsonSize: document_size_in_bytes,
     namespace: "db.collection", 
     yearMonth: "YYYYMM",
     shardId: "shard_identifier",
     chunkMin: chunk_minimum_boundary,
     chunkMax: chunk_maximum_boundary
   }
   ```
5. **Batch Processing**: Uses 1000-document batches for efficient insertion
6. **Progress Reporting**: Reports every 5000 documents processed

**Key Features**:
- Calculates exact BSON size for each document
- Maps documents to chunk boundaries for precise targeting
- Handles large datasets with memory-efficient streaming
- Provides detailed statistics (total docs, total size, average size)

### 2. Balance Mode (`--balance`)

**Purpose**: Move chunks containing large documents to achieve better distribution.

**Target Selection Algorithm**:

The balance phase uses this aggregation pipeline against the `mongoCustomBalancerStats.chunkStats` collection:

```javascript
db.chunkStats.aggregate([
  { 
    $match: { 
      bsonSize: { $gte: 1000000 },
      $or: [{ move: false }, { move: { $exists: false }}]
    }
  },
  { 
    $group: { 
      _id: "$chunkMin", 
      count: { $sum: 1 } 
    }
  },
  { 
    $sort: { count: -1 } 
  }
])
```

**Alternative Query (shows move status)**:
```javascript
db.chunkStats.aggregate([
  { 
    $match: { 
      bsonSize: { $gte: 1000000 }
    }
  },
  { 
    $group: { 
      _id: "$chunkMin", 
      count: { $sum: 1 },
      movedCount: { 
        $sum: { 
          $cond: [{ $eq: ["$move", true] }, 1, 0] 
        }
      }
    }
  },
  { 
    $sort: { count: -1 } 
  }
])
```

**Pipeline Breakdown**:
1. **$match**: Finds large documents (≥1MB BSON size) that haven't been moved yet
2. **$group**: Groups by chunk boundary (`chunkMin`) and counts large documents per chunk  
3. **$sort**: Orders chunks by document count (highest concentration first)

This identifies chunks with the most large documents, prioritizing those with the highest impact for balancing.

**Balancing Workflow**:
1. **Destination Setup**: Parse comma-separated destination shard indexes
2. **Chunk Identification**: Find chunks with most large documents using aggregation
3. **Round-Robin Distribution**: Distribute chunks across destination shards evenly
4. **Chunk Movement**: Execute `moveChunk` operations with retry logic
5. **Status Updates**: Mark moved documents with `move: true` flag
6. **Progress Tracking**: Real-time status reporting every 60 seconds

**Dry Run Mode**:
- Activated when `--dryRun` specified or no destination shards provided
- Shows top 100 chunks that would be moved
- No actual chunk movements performed
- Useful for impact assessment

**Status Reporting**:
```
📊 STATUS: 150 chunks processed, 142 successful moves, 8 splits | 
Runtime: 25m | Rate: 5.7 moves/min
```

### 3. Migration Mode (`--migrate`)

**Purpose**: Upgrade existing stats collection schema from `chunkId` to `chunkMin/chunkMax` format.

**Migration Process**:
1. **Schema Detection**: Find documents with `chunkId` but missing `chunkMin/chunkMax`
2. **Chunk Resolution**: Map `chunkId` to actual chunk boundaries using chunk map
3. **Batch Updates**: Update documents in 1000-document batches:
   ```javascript
   // Before migration
   { _id: "doc1", chunkId: "shard01_chunk_123", ... }
   
   // After migration  
   { _id: "doc1", chunkMin: {...}, chunkMax: {...}, ... }
   ```
4. **Verification**: Count remaining documents needing migration
5. **Progress Reporting**: Status updates every 5000 documents

## Technical Implementation Details

### Chunk Management

**Chunk Map Structure**:
```java
Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap
// namespace -> (min_boundary -> chunk_metadata)
```

**Chunk Lookup Algorithm**:
1. **Exact Match**: Try direct lookup using document's shard key
2. **Floor Entry**: Use `NavigableMap.floorEntry()` for range-based lookup
3. **Fallback**: Log warning if chunk not found

### Error Handling & Resilience

**Chunk Movement Retry Logic**:
- Maximum 10 retries per chunk
- Handles `ChunkTooBig` errors by triggering chunk splitting
- Uses exponential backoff for transient failures
- Tracks split operations via callback hooks

**Bulk Operation Error Handling**:
- Unordered bulk writes continue on individual failures
- Partial failure logging with error details
- Graceful degradation for non-critical operations

### Performance Optimizations

**Memory Management**:
- Streaming cursors for large result sets
- Batch processing (1000 documents) to limit memory usage
- Efficient chunk map lookup using `NavigableMap`

**Network Optimization**:
- Direct shard connections for targeted queries
- Bulk operations for metadata updates
- Aggregation pipelines for server-side filtering

### Status Tracking & Monitoring

**Real-time Metrics**:
- `chunksProcessed`: Total chunks analyzed
- `successfulMoves`: Completed chunk movements  
- `splitOperations`: Chunks split due to size constraints
- Processing rate (moves per minute)
- Total runtime

**Hook System**:
```java
protected void onChunkSplit() {
    splitOperations++; // Callback from parent Balancer class
}
```

## Configuration

### Command Line Interface

```bash
# Initialization
java -jar mongo-util.jar customAnalyzer --init \
  --shardIndex 0 \
  --yearMonth 202508 \
  --ns mydb.mycollection

# Balancing
java -jar mongo-util.jar customAnalyzer --balance \
  --ns mydb.mycollection \
  --destShardIndex "1,2,3"

# Dry Run  
java -jar mongo-util.jar customAnalyzer --balance \
  --ns mydb.mycollection \
  --dryRun

# Migration
java -jar mongo-util.jar customAnalyzer --migrate \
  --ns mydb.mycollection
```

### Configuration File
```properties
# Connection string for source MongoDB cluster
source=mongodb://mongos1:27017,mongos2:27017/
```

## Data Structures

### Stats Collection Schema
```javascript
{
  _id: ObjectId("..."),           // Document identifier
  bsonSize: 1500000,              // Document size in bytes
  namespace: "mydb.mycollection", // Target namespace
  yearMonth: "202508",            // Time period identifier
  shardId: "shard01",            // Current shard location
  chunkMin: { _id: "user:000" }, // Chunk minimum boundary
  chunkMax: { _id: "user:999" }, // Chunk maximum boundary
  move: false                     // Movement status flag
}
```

### Chunk Map Entry
```java
public class CountingMegachunk {
    private String id;           // Unique chunk identifier
    private BsonValue min;       // Minimum shard key boundary
    private BsonValue max;       // Maximum shard key boundary  
    private String shardId;      // Current shard location
    private int docCount;        // Document count estimate
}
```

## Workflow Integration

### Typical Operations Sequence
1. **Initial Analysis**: Run `--init` mode on source shard containing large documents
2. **Impact Assessment**: Use `--dryRun` to see balancing candidates
3. **Chunk Balancing**: Execute `--balance` with target destination shards
4. **Schema Migration**: Use `--migrate` if upgrading from older stats format
5. **Monitoring**: Track progress via status reports and final summary

### Best Practices
- Run initialization during low-traffic periods
- Use dry run mode before actual balancing
- Monitor chunk split operations for oversized chunks
- Verify move operations completed successfully
- Consider impact on application read/write patterns

## Error Scenarios & Recovery

### Common Issues
1. **Chunk Not Found**: Indicates stale chunk map, requires reloading
2. **ChunkTooBig Errors**: Triggers automatic chunk splitting
3. **Network Timeouts**: Handled by retry logic with exponential backoff
4. **Partial Bulk Write Failures**: Logged but don't stop processing

### Recovery Strategies
- Restart from last successful checkpoint
- Use stats collection `move` flag to skip completed operations
- Reload chunk maps if topology changes detected
- Monitor MongoDB logs for cluster-level issues

This architecture provides a robust, scalable solution for optimizing shard distribution in large MongoDB clusters, with particular focus on handling documents with significant size variations.