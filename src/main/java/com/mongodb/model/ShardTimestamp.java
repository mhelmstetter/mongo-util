package com.mongodb.model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import org.bson.BsonTimestamp;

public class ShardTimestamp {

    private String shardName;
    private BsonTimestamp timestamp;
    private ZonedDateTime date;

    public ShardTimestamp(String shardName, BsonTimestamp timestamp) {
        this.shardName = shardName;
        this.timestamp = timestamp;
        date = ZonedDateTime.ofInstant(Instant.ofEpochSecond(timestamp.getTime()), ZoneOffset.UTC);
    }
    
    public String getShardName() {
        return shardName;
    }

    public BsonTimestamp getTimestamp() {
        return timestamp;
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
