package com.mongodb.model;

import org.bson.BsonTimestamp;

public class ShardTimestamp {

    private String shardName;
    private BsonTimestamp timestamp;

    public ShardTimestamp(String shardName, BsonTimestamp timestamp) {
        this.shardName = shardName;
        this.timestamp = timestamp;
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
        builder.append("]");
        return builder.toString();
    }

}
