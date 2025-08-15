package com.mongodb.shardsync.command.drop;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "indexes", description = "Drop indexes on destination cluster")
public class DropIndexesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private DropCommand parent;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        boolean success = sync.dropIndexes();
        
        return success ? 0 : 1;
    }
}