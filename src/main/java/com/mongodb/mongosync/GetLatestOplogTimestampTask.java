package com.mongodb.mongosync;

import java.util.concurrent.Callable;

import org.bson.BsonTimestamp;

import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

public class GetLatestOplogTimestampTask implements Callable<ShardTimestamp> {
    
    private String shardName;
    private ShardClient client;
    
    public GetLatestOplogTimestampTask(String shardName, ShardClient client) {
        this.shardName = shardName;
        this.client = client;
    }

    @Override
    public ShardTimestamp call() throws Exception {
        BsonTimestamp ts = client.getLatestOplogTimestamp(shardName);
        return new ShardTimestamp(shardName, ts);
    }

}
