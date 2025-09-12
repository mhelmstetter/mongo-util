package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "metadata", description = "Synchronize shard metadata between clusters")
public class SyncMetadataCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private SyncCommand parent;
    
    @Option(names = {"--skipOptimizeAdjacent"}, 
            description = "Skip optimization that will combine adjacent chunks")
    private boolean skipOptimizeAdjacent;
    
    @Option(names = {"--legacy"}, 
            description = "Use legacy metadata sync method (slower)", 
            hidden = true)
    private boolean legacy;
    
    @Option(names = {"--skipFlushRouterConfig"}, 
            description = "Skip the flushRouterConfig step")
    private boolean skipFlushRouterConfig;
    
    @Option(names = {"--force"}, 
            description = "Skip preflight checks and force sync operation")
    private boolean force;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setSkipFlushRouterConfig(skipFlushRouterConfig);
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        boolean success = true;
        
        if (legacy) {
            success = sync.syncMetadataLegacy(force);
        } else if (skipOptimizeAdjacent) {
        	success = sync.syncMetadata(force);
        } else {
        	success = sync.syncMetadataOptimized(force);
        }
        
        return success ? 0 : 1;
    }
}