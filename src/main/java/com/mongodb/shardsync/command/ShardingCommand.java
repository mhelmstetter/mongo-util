package com.mongodb.shardsync.command;

import com.mongodb.shardsync.ShardConfigSyncApp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "sharding", description = "Sharding operations and maintenance")
public class ShardingCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @Option(names = {"--flushRouter"}, description = "Flush router configuration")
    private boolean flushRouter;
    
    @Option(names = {"--disableSourceAutosplit"}, description = "Disable autosplit on source cluster")
    private boolean disableSourceAutosplit;
    
    
    @Option(names = {"--checkShardedIndexes"}, description = "Check sharded indexes")
    private boolean checkShardedIndexes;
    
    @Option(names = {"--cleanupOrphans"}, description = "Clean up orphaned documents")
    private boolean cleanupOrphans;
    
    @Option(names = {"--cleanupOrphansDest"}, description = "Clean up orphaned documents on destination")
    private boolean cleanupOrphansDest;
    
    @Option(names = {"--cleanupOrphansSleep"}, description = "Sleep duration between cleanup operations (milliseconds)")
    private String cleanupOrphansSleep;
    
    @Override
    public Integer call() throws Exception {
        return parent.executeShardingCommand(this);
    }
    
    // Getters for the parent class to access the fields
    public boolean isFlushRouter() { return flushRouter; }
    public boolean isDisableSourceAutosplit() { return disableSourceAutosplit; }
    public boolean isShardCollections() { return false; }
    public boolean isCheckShardedIndexes() { return checkShardedIndexes; }
    public boolean isCleanupOrphans() { return cleanupOrphans; }
    public boolean isCleanupOrphansDest() { return cleanupOrphansDest; }
    public String getCleanupOrphansSleep() { return cleanupOrphansSleep; }
}