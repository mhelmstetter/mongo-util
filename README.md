# MongoDB Utilities

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
