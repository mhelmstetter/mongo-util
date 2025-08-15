package com.mongodb.shardsync.command.compare;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "indexes", description = "Compare indexes between source and destination")
public class CompareIndexesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private CompareCommand parent;
    
    @Option(names = {"--ttlOnly"}, 
            description = "Only compare TTL indexes")
    private boolean ttlOnly;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        return sync.compareIndexes(false, ttlOnly);
    }
}