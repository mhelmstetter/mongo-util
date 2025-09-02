package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "indexes", description = "Copy indexes from source to destination")
public class SyncIndexesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private SyncCommand parent;
    
    @Option(names = {"--extendTtl"}, 
            description = "Extend TTL expiration")
    private boolean extendTtl;
    
    @Option(names = {"--ttlOnly"}, 
            description = "Only sync TTL indexes")
    private boolean ttlOnly;
    
    @Option(names = {"--collModTtl"}, 
            description = "Use collMod to update TTL indexes on destination based on source")
    private boolean collModTtl;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setExtendTtl(extendTtl);
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (collModTtl) {
            // When using collModTtl, force ttlOnly=true since collMod can only modify TTL settings
            return sync.compareIndexes(true, true);
        } else {
            sync.syncIndexesShards(true, extendTtl, ttlOnly);
            return 0;
        }
    }
}