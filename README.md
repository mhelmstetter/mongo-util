# MongoDB Utilities

ShardConfigSync
---------------
java -cp mongo-util.jar com.mongodb.shardsync.ShardConfigSyncApp -dropDestinationCollectionsIfExisting -s mongodb://admin:password@mongos-host:27017/?authSource=admin -d mongodb://admin:admin@mongos-host:27016/?ssl=true&authSource=admin

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
