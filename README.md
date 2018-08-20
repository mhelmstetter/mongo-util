# MongoDB Utilities

ShardConfigSync
---------------
Sync shard metadata

```
java -cp mongo-util.jar com.mongodb.shardsync.ShardConfigSyncApp \
    -s mongodb://admin:mypassword@source:27017/?authSource=admin \
    -d mongodb://destination1:27017 \
    -f db1 -f db2 \
    -m sh_0|shard_A -m sh_1|shard_A -m sh_2|shard_B -m sh_3|shard_B \
    -p /Users/mh/go/src/github.com/10gen/mongomirror/build/mongomirror \
    -dropDestinationCollectionsIfExisting \
    -syncMetadata
```

Run mongomirror process for each shard

```
java -cp mongo-util.jar com.mongodb.shardsync.ShardConfigSyncApp \
    -s mongodb://admin:mypassword@source:27017/?authSource=admin \
    -d mongodb://destination1:27017 \
    -f db1 -f db2 \
    -m sh_0|shard_A -m sh_1|shard_A -m sh_2|shard_B -m sh_3|shard_B \
    -p /Users/mh/go/src/github.com/10gen/mongomirror/build/mongomirror \
    -dropDestinationCollectionsIfExisting \
    -mongomirror
```

MongoReplayFilter
-----------------
Download:
```
wget -O mongo-util.jar https://github.com/mhelmstetter/mongo-util/blob/master/bin/mongo-util.jar?raw=true
```

Run:
```
java -cp mongo-util.jar com.mongodb.mongoreplay.MongoReplayFilter record041020185.bson
```
The output file will be written with the same name + `.FILTERED`.


DiffUtil
-----------------
Download:
```
wget -O mongo-util.jar https://github.com/mhelmstetter/mongo-util/blob/master/bin/mongo-util.jar?raw=true
```

Run:
```
java -cp mongo-util.jar com.mongodb.diffutil.DiffUtilApp
```
