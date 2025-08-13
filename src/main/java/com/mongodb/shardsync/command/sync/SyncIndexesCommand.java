package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.SyncCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "indexes", description = "Copy indexes from source to destination")
public class SyncIndexesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private SyncCommand parent;
    
    @Option(names = {"--collation"}, 
            description = "Collation to apply to index creation")
    private String collation;
    
    @Option(names = {"--extend-ttl"}, 
            description = "Extend TTL expiration")
    private boolean extendTtl;
    
    @Option(names = {"--ttl-only"}, 
            description = "Only sync TTL indexes")
    private boolean ttlOnly;
    
    @Option(names = {"--collmod-ttl"}, 
            description = "Use collMod to update TTL indexes on destination based on source")
    private boolean collModTtl;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setExtendTtl(extendTtl);
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (collModTtl) {
            return sync.compareIndexes(true);
        } else {
            sync.syncIndexesShards(true, extendTtl, collation, ttlOnly);
            return 0;
        }
    }
}