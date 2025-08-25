package com.mongodb.shardsync.command.compare;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "uuids", description = "Compare collection UUIDs between clusters")
public class CompareUuidsCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private CompareCommand parent;
    
    @Option(names = {"--dropEmptyOnIncorrectShards"}, 
            description = "Drop empty collections having UUID mismatch")
    private boolean dropEmptyOnIncorrectShards;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setDrop(dropEmptyOnIncorrectShards);
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        sync.compareCollectionUuids();
        
        return 0;
    }
}