package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.SyncMetadataResult;

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
        
        SyncMetadataResult result;
        
        if (legacy) {
            result = sync.syncMetadataLegacy(force);
        } else if (skipOptimizeAdjacent) {
        	result = sync.syncMetadata(force);
        } else {
        	result = sync.syncMetadataOptimized(force);
        }
        
        // Return proper exit codes based on results
        if (result.isOverallSuccess()) {
            if (result.hasWarnings()) {
                // Success with warnings - exit code 0 but user should review warnings
                return 0;
            } else {
                // Complete success
                return 0;
            }
        } else {
            // Failure - exit code 1
            return 1;
        }
    }
}