# MongoDB Balancer Internals Documentation

## Overview

This document provides a comprehensive reference for understanding MongoDB's internal balancer implementation. This is intended as a guide for implementing custom/external balancers that need to understand the decision-making process, algorithms, and specific nuances of the built-in balancer.

**Code Location**: `/Users/mh/git/mongo/src/mongo/db/s/balancer/`

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Balancer Main Loop](#balancer-main-loop)
3. [Chunk Selection Algorithm](#chunk-selection-algorithm)
4. [Range Determination for moveRange](#range-determination-for-moverange)
5. [Zone-Aware Balancing](#zone-aware-balancing)
6. [Data Size Calculation](#data-size-calculation)
7. [Migration Execution](#migration-execution)
8. [Important Nuances and Edge Cases](#important-nuances-and-edge-cases)

---

## Architecture Overview

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| **Balancer** | `balancer.cpp` | Main orchestrator, runs balancing rounds |
| **BalancerChunkSelectionPolicy** | `balancer_chunk_selection_policy.cpp` | Selects which collections and chunks to migrate |
| **BalancerPolicy** | `balancer_policy.cpp` | Core algorithm for determining migrations |
| **DistributionStatus** | `balancer_policy.cpp` | Tracks chunk distribution across shards and zones |
| **BalancerCommandsScheduler** | `balancer_commands_scheduler_impl.cpp` | Executes migration commands |

### Thread Architecture

The balancer runs two main threads:

1. **Main Balancer Thread** (`_mainThread()` - balancer.cpp:991-1228)
   - Executes balancing rounds
   - Handles chunk migrations for rebalancing
   - Manages defragmentation

2. **Action Stream Consumer Thread** (`_consumeActionStreamLoop()` - balancer.cpp:809-989)
   - Handles streaming operations (merges, data size requests)
   - Processes defragmentation policy actions
   - Runs auto-merger policy

---

## Balancer Main Loop

**Location**: `balancer.cpp:991-1228` (`Balancer::_mainThread()`)

### Balancer Round Lifecycle

```
1. _beginRound()
2. Check OIDs
3. Refresh balancer configuration
4. Check if balancing is enabled
5. Start collection defragmentations
6. Split chunks (zone enforcement)
7. Select and migrate chunks
   a. Get cluster statistics
   b. Select unsharded collections to move
   c. Select chunks to defragment
   d. Select chunks to rebalance
   e. Execute migrations
8. _endRound()
9. Sleep (throttle or kBalanceRoundDefaultInterval = 10 seconds)
```

### Configuration Checks

Before each round, the balancer verifies (balancer.cpp:1053-1068):
- Balancer mode is enabled (`balancerConfig->shouldBalance()`)
- Not in termination state
- AutoMerger requirements (`shouldBalanceForAutoMerge()`)

### Key Configuration Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `kBalanceRoundDefaultInterval` | 10 seconds | Time between balancing rounds when no work |
| `balancerMigrationsThrottlingMs` | Variable | Minimum time between migrations |
| `balancerChunksSelectionTimeoutMs` | Variable | Max time to spend selecting chunks |
| `chunkDefragmentationThrottlingMS` | Variable | Throttle for defragmentation actions |

---

## Chunk Selection Algorithm

**Location**: `balancer_chunk_selection_policy.cpp:352-542`

### High-Level Flow

```
BalancerChunkSelectionPolicy::selectChunksToMove()
  └─> For each collection (in batches):
      └─> _getMigrateCandidatesForCollection()
          └─> Create DistributionStatus
          └─> Call BalancerPolicy::balance()
```

### Collection Batching

**Location**: `balancer_chunk_selection_policy.cpp:376-508`

The balancer processes collections in batches to optimize data size queries:

- **Batch Size**: Default 100 collections (`kStatsForBalancingBatchSize`)
- **Cache Size**: 75% of batch size for imbalanced collections (`kMaxCachedCollectionsSize`)
- **Prioritization**: Cached imbalanced collections processed first

#### Imbalanced Collections Cache

The balancer maintains a cache of collections that had migrations in previous rounds:
- Collections with migrations are added to cache (line 443)
- Collections without migrations are removed from cache (line 441)
- Cache is limited to prevent unbounded growth
- Cache provides "stickiness" - recently imbalanced collections are checked first

### Collection Filtering

Collections are skipped if (balancer_policy.cpp:273-288):
```cpp
!coll.getAllowBalance()          // Balancing explicitly disabled
!coll.getAllowMigrations()       // Migrations disabled
!coll.getPermitMigrations()      // Migrations not permitted
coll.getDefragmentCollection()   // Currently defragmenting
```

---

## Range Determination for moveRange

**Location**: `balancer_policy.cpp:449-737` (`BalancerPolicy::balance()`)

### Critical Insight: Chunks Are Atomic Units

⚠️ **The balancer NEVER specifies arbitrary ranges**. It always moves **entire existing chunks** by their natural boundaries.

### Three-Phase Priority Algorithm

The `balance()` function returns migrations following this strict priority order:

#### Phase 1: Drain Shards (Lines 458-549)

**Priority**: HIGHEST

**Algorithm**:
```
For each draining shard:
  For each zone on that shard:
    For each chunk in (shard, zone):
      If chunk is jumbo:
        Skip (log warning)
      Else:
        Select this chunk
        Find least loaded receiver in zone
        Create migration
        Return immediately (one chunk per round)
```

**Key Characteristics**:
- Always uses `ForceJumbo::kForceBalancer` to force jumbo chunk movement
- Stops after finding first non-jumbo chunk
- Removes both source and destination shards from available pool

**Code Reference**: balancer_policy.cpp:505-515
```cpp
migrations.emplace_back(
    to,
    chunk.getShardId(),
    distribution.nss(),
    distribution.getChunkManager().getUUID(),
    chunk.getMin(),
    boost::none /* max */,  // Max is implicit from chunk metadata
    chunk.getLastmod(),
    ForceJumbo::kForceBalancer,
    collDataSizeInfo.maxChunkSizeBytes);
```

#### Phase 2: Zone Violations (Lines 575-669)

**Priority**: HIGH

**Algorithm**:
```
For each shard:
  For each zone on shard:
    If shard is not assigned to this zone:
      For each chunk in (shard, zone):
        If chunk is jumbo:
          Log warning, continue
        Else:
          Select this chunk
          Find least loaded receiver in proper zone
          Create migration
          Return immediately
```

**Key Characteristics**:
- Corrects zone placement errors
- Jumbo chunks logged but skipped (balancer_policy.cpp:601-611)
- Uses `forceJumbo` parameter from config (not forced by default)

#### Phase 3: Load Balancing (Lines 671-734)

**Priority**: NORMAL

**Algorithm**:
```
For each zone (including "no zone"):
  Calculate ideal data size per shard:
    totalDataSizeInZone / numShardsInZone

  While _singleZoneBalanceBasedOnDataSize() returns true:
    Find most overloaded shard in zone
    Find least loaded shard in zone

    If fromSize > idealSize AND toSize < idealSize:
      If (fromSize - toSize) >= 3 * maxChunkSize:
        Select first non-jumbo chunk from overloaded shard
        Create migration to least loaded shard
        Remove both shards from available pool
```

**Key Decision Point** (balancer_policy.cpp:781-793):
```cpp
if (fromSize <= idealDataSizePerShardForZone) {
    return false;  // Source not overloaded
}

if (toSize >= idealDataSizePerShardForZone) {
    return false;  // Destination already has enough data
}

if (fromSize - toSize < 3 * collDataSizeInfo.maxChunkSizeBytes) {
    return false;  // Difference too small to warrant migration
}
```

### Chunk Selection Within a Shard

**Location**: balancer_policy.cpp:802-824

When multiple chunks exist on a shard, the balancer uses a **greedy first-match** strategy:

```cpp
const auto chunkFound =
    !distribution.forEachChunkOnShardInZone(fromShardId, zone, [&](const auto& chunk) {
        if (chunk.isJumbo()) {
            numJumboChunks++;
            return true;  // continue iterating
        }

        // First non-jumbo chunk - take it immediately
        migrations->emplace_back(toShardId, chunk.getShardId(), ...);
        return false;  // break - we're done
    });
```

**Important**:
- No size-based selection of individual chunks
- No optimization for "best chunk to move"
- Simply takes the first non-jumbo chunk encountered
- Iteration order determined by ChunkManager's internal structure

### Range Specification in MigrateInfo

The range is specified using:
- **minKey**: Chunk's minimum bound (required)
- **maxKey**: `boost::none` (optional) - derived from chunk metadata
- **version**: Chunk's lastmod version
- **uuid**: Collection UUID

**Why maxKey is optional**: The chunk version uniquely identifies the chunk's boundaries in the routing table.

---

## Zone-Aware Balancing

**Location**: `balancer_policy.cpp:94-135` (`normalizeZones()`)

### Zone Normalization

Zones are "normalized" to ensure they don't partially overlap chunks:

```
Original Zone: [10, 50)
Chunk Boundaries: [0, 25), [25, 75), [75, 100)

Normalized Zone: [25, 50)
  - Lower bound adjusted to chunk boundary (25)
  - Upper bound at chunk boundary (50 falls within [25, 75), so normalized to 25)
```

### "No Zone" Handling

The balancer creates synthetic "no zone" ranges for key ranges not covered by explicit zones:

```cpp
if (SimpleBSONObjComparator::kInstance.evaluate(normalizedMin != lastMax)) {
    // Gap between zones - create kNoZoneName range
    normalizedRanges.emplace_back(lastMax, normalizedMin, ZoneInfo::kNoZoneName);
}
```

**Key Behavior**:
- Shards without explicit zone assignment can hold "no zone" chunks
- "No zone" chunks are balanced separately from zoned chunks
- All shards participate in "no zone" balancing

### Zone Validation

Before balancing, the policy validates zones don't split chunks (balancer_chunk_selection_policy.cpp:608-641):

```cpp
if (chunkAtZoneMin.getMin().woCompare(zoneRange.min)) {
    return {ErrorCodes::IllegalOperation,
            str::stream() << "Zone boundaries " << zoneRange.toString()
                          << " fall in the middle of an existing chunk"};
}
```

**Action if validation fails**: Collection balancing postponed until chunks are split appropriately.

---

## Data Size Calculation

**Location**: `balancer_policy.cpp:990-1044` (`getStatsForBalancing()`)

### Size-Based Balancing (Not Chunk Count)

⚠️ **Critical**: The balancer balances based on **data size in bytes**, NOT chunk count.

### Data Collection Process

1. **Request Construction** (line 995-997):
```cpp
ShardsvrGetStatsForBalancing req{namespacesWithUUIDsForStatsRequest};
req.setScaleFactor(1);
const auto reqObj = req.toBSON();
```

2. **Parallel Shard Queries** (line 1000-1001):
```cpp
auto responsesFromShards = sharding_util::sendCommandToShards(
    opCtx, DatabaseName::kAdmin, reqObj, shardIds, executor, false);
```

3. **Response Aggregation** (line 1021-1023):
```cpp
for (const auto& stats : collStatsFromShard) {
    namespaceToShardDataSize[stats.getNs()][shardId] = stats.getCollSize();
}
```

### Data Structure

```cpp
// Maps: namespace -> (shardId -> dataSize)
using NamespaceStringToShardDataSizeMap =
    stdx::unordered_map<NamespaceString, ShardDataSizeMap>;

// Maps: shardId -> dataSize in bytes
using ShardDataSizeMap = std::map<ShardId, int64_t>;
```

### Ideal Shard Size Calculation

**Location**: balancer_policy.cpp:718-719

```cpp
const int64_t idealDataSizePerShardForZone =
    totalDataSizeOfShardsWithZone / numShardsInZone;
```

**Per-Zone Calculation**:
- Each zone (including "no zone") calculates its own ideal size
- Shards participate in multiple zone calculations if assigned to multiple zones
- Shard can be above ideal in one zone and below ideal in another

---

## Migration Execution

**Location**: `balancer.cpp:1376-1416` (`Balancer::_doMigrations()`)

### Migration Types

The balancer handles three types of migrations:

1. **Unsharded Collection Moves** (`MigrateUnshardedCollectionTask`)
   - Uses `requestMoveCollection()`
   - Enqueued via `enqueueCollectionMigrations()` (balancer.cpp:373-388)

2. **Chunk Rebalancing** (`RebalanceChunkTask`)
   - Uses `requestMoveRange()`
   - Enqueued via `enqueueChunkMigrations()` (balancer.cpp:390-430)

3. **Defragmentation** (`DefragmentChunkTask`)
   - Uses `requestMoveRange()`
   - Enqueued via `enqueueChunkMigrations()`

### Parallel Execution

All migrations within a round are enqueued in parallel:

```cpp
std::array<std::unique_ptr<MigrationTask>, 3> allMigrationTasks = {...};

// Enqueue all
for (const auto& migrationTask : allMigrationTasks) {
    migrationTask->enqueue();
}

// Wait for all
for (const auto& migrationTask : allMigrationTasks) {
    migrationTask->waitForQueuedAndProcessResponses();
}
```

### MoveRange Request Construction

**Location**: balancer.cpp:407-424

```cpp
MoveRangeRequestBase requestBase(migrateInfo.to);
requestBase.setWaitForDelete(balancerConfig->waitForDelete());
requestBase.setMin(migrateInfo.minKey);
requestBase.setMax(migrateInfo.maxKey);

ShardsvrMoveRange shardSvrRequest(migrateInfo.nss);
shardSvrRequest.setDbName(DatabaseName::kAdmin);
shardSvrRequest.setMoveRangeRequestBase(requestBase);
shardSvrRequest.setMaxChunkSizeBytes(maxChunkSizeBytes);
shardSvrRequest.setFromShard(migrateInfo.from);
shardSvrRequest.setCollectionTimestamp(migrateInfo.version.getTimestamp());
shardSvrRequest.setForceJumbo(migrateInfo.forceJumbo);
```

### Secondary Throttle

**Location**: balancer.cpp:324-336

```cpp
std::tuple<bool, WriteConcernOptions> getSecondaryThrottleAndWriteConcern(
    const boost::optional<MigrationSecondaryThrottleOptions>& secondaryThrottle) {
    if (secondaryThrottle &&
        secondaryThrottle->getSecondaryThrottle() == MigrationSecondaryThrottleOptions::kOn) {
        if (secondaryThrottle->isWriteConcernSpecified()) {
            return {true, secondaryThrottle->getWriteConcern()};
        }
        return {true, WriteConcernOptions()};
    }
    return {false, WriteConcernOptions()};
}
```

### Migration Failure Handling

**Location**: balancer.cpp:441-495

#### ChunkTooBig / ExceededMemoryLimit

```cpp
if (status == ErrorCodes::ChunkTooBig || status == ErrorCodes::ExceededMemoryLimit) {
    // Attempt to split the chunk
    ShardingCatalogManager::get(opCtx)->splitOrMarkJumbo(
        opCtx, collection.getNss(), migrateInfo.minKey, migrateInfo.getMaxChunkSizeBytes());
    return true;  // Count as success (split attempted)
}
```

#### IndexNotFound on Hashed Collections

```cpp
if (status == ErrorCodes::IndexNotFound) {
    const auto cm = uassertStatusOK(getPlacementInfoForShardedCollection(opCtx, migrateInfo.nss));

    if (cm.getShardKeyPattern().isHashedPattern()) {
        // Disable balancing for this collection
        commandScheduler.disableBalancerForCollection(opCtx, migrateInfo.nss);
        return false;
    }
}
```

### Post-Migration Verification

**Location**: balancer.cpp:276-312 (`processManualMigrationOutcome()`)

After a migration, the balancer verifies success by checking the routing table:

```cpp
const auto currentChunkInfo =
    min ? cm.findIntersectingChunkWithSimpleCollation(*min)
        : getChunkForMaxBound(cm, *max);

if (currentChunkInfo.getShardId() == destination &&
    outcome != ErrorCodes::BalancerInterrupted) {
    // Chunk is on destination despite error - metadata was committed
    outcome = Status::OK();
}
```

**Nuance**: A migration can fail after metadata commit but before waitForDelete completes.

---

## Important Nuances and Edge Cases

### 1. Shard Availability Tracking

**Location**: balancer.cpp:1108-1116

```cpp
stdx::unordered_set<ShardId> availableShards;
availableShards.reserve(shardStats.size());
std::transform(
    shardStats.begin(),
    shardStats.end(),
    std::inserter(availableShards, availableShards.end()),
    [](const ClusterStatistics::ShardStatistics& shardStatistics) -> ShardId {
        return shardStatistics.shardId;
    });
```

**Behavior**:
- Both source and destination shards are removed after each migration
- Prevents multiple simultaneous migrations involving the same shard
- Limits migrations per round to `floor(numShards / 2)`

### 2. Jumbo Chunk Handling

**Definition**: A chunk marked as "jumbo" cannot be moved (unless draining).

**Scenarios**:
1. **Draining Shard**: Jumbo chunks ARE moved (`ForceJumbo::kForceBalancer`)
2. **Zone Violation**: Jumbo chunks are skipped (logged as warning)
3. **Load Balancing**: Jumbo chunks are skipped (counted but not moved)

**Code**: balancer_policy.cpp:478-481, 601-611, 803-806

### 3. The "3x MaxChunkSize" Rule

**Location**: balancer_policy.cpp:790-793

```cpp
if (fromSize - toSize < 3 * collDataSizeInfo.maxChunkSizeBytes) {
    // Do not balance if the collection's size differs too few between the chosen shards
    return false;
}
```

**Rationale**: Prevents migrations when the imbalance is less than 3 chunks worth of data, avoiding churn for minimal benefit.

**Impact**: Small collections (< 3 chunks per shard difference) won't trigger migrations.

### 4. Zone Assignment Edge Cases

**Empty Zones** (balancer_policy.cpp:694-704):
```cpp
if (numShardsInZone == 0) {
    if (zone != ZoneInfo::kNoZoneName) {
        LOGV2_WARNING(21893, "Zone in collection has no assigned shards...");
    }
    continue;
}
```

**Behavior**: Zones without assigned shards are skipped with a warning.

### 5. Chunk Split Before Balance

**Location**: balancer.cpp:1091-1099

```cpp
// Split chunk to match zones boundaries
{
    Status status = _splitChunksIfNeeded(opCtx.get());
    if (!status.isOK()) {
        LOGV2_WARNING(21878, "Failed to split chunks", "error"_attr = status);
    } else {
        LOGV2_DEBUG(21861, 1, "Done enforcing zone range boundaries.");
    }
}
```

**Critical Sequence**:
1. Splits are attempted BEFORE chunk selection
2. Split failures don't block the round (warning only)
3. Collections with zone boundary violations will be skipped during selection

### 6. Collection Namespace Validation

Collections can be dropped between:
- Fetching the collection list
- Attempting to get placement info

**Handling**: `ErrorCodes::NamespaceNotFound` is caught and collection is skipped (balancer_chunk_selection_policy.cpp:319-321, 422-425).

### 7. Balancer vs Manual moveRange

**Location**: balancer.cpp:749-794

When called via `moveRange()` (manual operation):
- User specifies namespace and min/max bounds
- Balancer determines source shard from routing table
- Same `ShardsvrMoveRange` command used
- Respects same throttle and secondary throttle settings

**Key Difference**: Manual moves can violate balancing policy (zones, load).

### 8. OID Machine Check

**Location**: balancer.cpp:1271-1341 (`_checkOIDs()`)

The balancer performs ObjectId machine ID uniqueness checks:
- Detects if multiple shards have same OID machine ID
- Resets OID generators if conflict detected
- Fails the round if conflict exists

**Nuance**: This is a safety check for ObjectId uniqueness guarantees.

### 9. Imbalanced Collection Cache Behavior

The cache provides "stickiness" but has limits:
- Max size: 75 collections (default)
- Collections removed when balanced
- Collections removed if dropped or disabled
- Cache cleared on balancer restart

**Impact**: Recently active collections get priority, but cache limits prevent starvation of other collections.

### 10. Configuration Refresh Timing

**Location**: balancer.cpp:1020-1032, 1053-1058

Configuration is refreshed:
- At balancer startup
- Before each round
- Failures cause round skip with backoff

**Nuance**: Configuration changes take effect within one round interval (10 seconds default).

---

## Testing and Debug Tools

### Failpoints

- `balancerShouldReturnRandomMigrations`: Force random migrations for testing
- `overrideStatsForBalancingBatchSize`: Override collection batch size
- `forceBalancerWarningChecks`: Force draining shard warning checks

### Balancer Status

**Command**: `balancerCollectionStatus`

**Returns**:
- `maxChunkSizeMB`: Configured max chunk size
- `balancerCompliant`: True if no migrations needed
- `firstComplianceViolation`: Reason if not compliant
  - `"draining"`: Shard is draining
  - `"zoneViolation"`: Zone placement error
  - `"chunksImbalance"`: Load imbalance
  - `"defragmentingChunks"`: Currently defragmenting

### Logging

**Component**: `::mongo::logv2::LogComponent::kSharding`

**Key Log IDs**:
- `21856`: Balancer starting
- `21860`: Start balancing round
- `21862`: No chunks to move
- `21865`: Error during balance
- `21889`: Chunk on draining shard, no recipient
- `21890`: Unable to drain shard (jumbo chunks)
- `21891`: Jumbo chunk violates zone
- `21893`: Zone has no assigned shards

---

## Migration State Machine

```
[Balancer selects chunk]
         |
         v
[Create MigrateInfo with minKey/maxKey]
         |
         v
[Construct ShardsvrMoveRange command]
         |
         v
[Send to source shard via BalancerCommandsScheduler]
         |
         v
[Source shard executes migration]
         |
         +---> [Success] --> Verify chunk location
         |
         +---> [ChunkTooBig] --> Split chunk
         |
         +---> [IndexNotFound + Hashed] --> Disable balancing
         |
         +---> [Other Error] --> Log failure
         |
         v
[Process response, update metrics]
         |
         v
[Remove namespaces from imbalanced cache if failed]
```

---

## Key Takeaways for Custom Balancer Implementation

### Must Implement

1. **Shard Statistics Collection**: Query all shards for data sizes per collection
2. **Zone Awareness**: Respect zone assignments and constraints
3. **Priority Ordering**: Drain > Zone Violation > Load Balance
4. **Chunk Boundary Respect**: Never split existing chunk ranges
5. **Jumbo Handling**: Skip jumbo chunks (except draining)
6. **Parallel Execution**: Enqueue multiple migrations per round
7. **Error Recovery**: Handle chunk splits, index errors, namespace drops

### Recommended Behaviors

1. **Batch Operations**: Process collections in batches for efficiency
2. **Cache Imbalanced Collections**: Prioritize recently imbalanced collections
3. **3x Rule**: Only migrate if difference >= 3x max chunk size
4. **Shard Exclusion**: Don't reuse shards in same round
5. **Throttling**: Respect migration throttle settings
6. **Verification**: Verify chunk location after migration

### Can Customize

1. **Chunk Selection Strategy**: Built-in uses first-match; could optimize
2. **Collection Prioritization**: Could use different metrics
3. **Zone Balancing**: Could use different thresholds
4. **Batch Sizes**: Tune for your cluster size
5. **Round Interval**: Adjust based on workload

### Must NOT Do

1. ❌ Split chunk ranges arbitrarily
2. ❌ Ignore zone constraints (except when fixing violations)
3. ❌ Move jumbo chunks during normal balancing
4. ❌ Migrate to draining shards
5. ❌ Process collections with `allowBalance: false`
6. ❌ Skip zone boundary validation

---

## Appendix: Key Code Locations Quick Reference

| Functionality | File:Line |
|---------------|-----------|
| Main balancer loop | balancer.cpp:991-1228 |
| Chunk selection entry | balancer_chunk_selection_policy.cpp:352-509 |
| Balance algorithm | balancer_policy.cpp:449-737 |
| Single zone balance | balancer_policy.cpp:739-836 |
| Drain shard logic | balancer_policy.cpp:458-549 |
| Zone violation logic | balancer_policy.cpp:575-669 |
| Load balance logic | balancer_policy.cpp:671-734 |
| Data size collection | balancer_policy.cpp:990-1044 |
| Migration execution | balancer.cpp:1376-1416 |
| MoveRange construction | balancer.cpp:390-430 |
| Migration failure handling | balancer.cpp:441-495 |
| Zone normalization | balancer_policy.cpp:94-135 |
| Split enforcement | balancer.cpp:1343-1374 |

---

**Document Version**: 1.0
**MongoDB Version**: master (as of commit 3991b066ab0)
**Last Updated**: 2025-09-29
