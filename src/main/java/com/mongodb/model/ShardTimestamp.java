package com.mongodb.model;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;

import com.google.gson.JsonObject;

public class ShardTimestamp {

    private String shardName;
    private BsonTimestamp timestamp;
    private ZonedDateTime date;
    private BsonDocument oplogEntry;

    public ShardTimestamp(String shardName, BsonTimestamp timestamp) {
        this.shardName = shardName;
        this.timestamp = timestamp;
        date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp.getTime()), ZoneOffset.UTC);
    }
    
    public ShardTimestamp(Shard shard, BsonDocument oplogEntry) {
        this.shardName = shard.getRsName();
        this.timestamp = oplogEntry.getTimestamp("ts");
        this.oplogEntry = oplogEntry;
        date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp.getTime()), ZoneOffset.UTC);
    }
    
    public String getShardName() {
        return shardName;
    }

    public BsonTimestamp getTimestamp() {
        return timestamp;
    }
    
    public String toJsonString() {
    	JsonObject j = new JsonObject();
    	j.addProperty("replicaSetName", shardName);
    	JsonObject opTime = new JsonObject();
    	j.add("opTime", opTime);
    	JsonObject ts = new JsonObject();
    	ts.addProperty("T", timestamp.getTime());
    	ts.addProperty("I", timestamp.getInc());
    	opTime.add("timestamp", ts);
    	opTime.addProperty("term", oplogEntry.getNumber("t").intValue());
    	opTime.addProperty("hash", oplogEntry.getNumber("h").intValue());
    	
    	
    	return j.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ShardTimestamp [shardName=");
        builder.append(shardName);
        builder.append(", timestamp=");
        builder.append(timestamp);
        builder.append(", date=");
        builder.append(date);
        builder.append("]");
        return builder.toString();
    }

}
