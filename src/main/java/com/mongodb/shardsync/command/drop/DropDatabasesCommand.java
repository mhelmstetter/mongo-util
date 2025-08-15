package com.mongodb.shardsync.command.drop;

import java.util.concurrent.Callable;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "databases", description = "Drop databases on destination cluster")
public class DropDatabasesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private DropCommand parent;
    
    @Option(names = {"--shardDatabases"}, 
            description = "Drop databases directly on shards rather than through mongos",
            hidden = true)
    private boolean shardDatabases;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (shardDatabases) {
            sync.dropDestinationDatabases();
        } else {
            sync.dropDestinationDatabasesAndConfigMetadata();
        }
        
        return 0;
    }
}