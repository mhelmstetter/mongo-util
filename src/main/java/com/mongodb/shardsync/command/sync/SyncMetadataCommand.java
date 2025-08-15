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
    
    @Option(names = {"--optimized"}, 
            description = "Combine adjacent chunks for optimization (default: true)", 
            defaultValue = "true")
    private boolean optimized = true;
    
    @Option(names = {"--legacy"}, 
            description = "Use legacy metadata sync method (slower)", 
            hidden = true)
    private boolean legacy;
    
    @Option(names = {"--skipFlushRouterConfig"}, 
            description = "Skip the flushRouterConfig step")
    private boolean skipFlushRouterConfig;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setSkipFlushRouterConfig(skipFlushRouterConfig);
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        boolean success = true;
        
        if (legacy) {
            sync.syncMetadataLegacy();
            // Legacy method doesn't return status, assume success
        } else if (optimized) {
            success = sync.syncMetadataOptimized();
        } else {
            success = sync.syncMetadata();
        }
        
        return success ? 0 : 1;
    }
}