package com.mongodb.shardsync.command.compare;

import com.mongodb.shardsync.ShardConfigSyncApp;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.AtlasOptionsMixin;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "compare", 
         description = "Compare source and destination clusters",
         subcommands = {
             CompareChunksCommand.class,
             CompareIndexesCommand.class,
             CompareUsersCommand.class,
             CompareUuidsCommand.class
             // Add more subcommands as needed
         })
public class CompareCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @CommandLine.Mixin
    private AtlasOptionsMixin atlasOptions = new AtlasOptionsMixin();
    
    // Keep legacy options for backward compatibility
    @Option(names = {"--collCounts"}, description = "Compare collection counts", hidden = true)
    private boolean collCounts;
    
    @Option(names = {"--compareDbMeta"}, description = "Compare database metadata", hidden = true)
    private boolean compareDbMeta;
    
    @Option(names = {"--compareCollectionUuids"}, description = "Compare collection UUIDs", hidden = true)
    private boolean compareCollectionUuids;
    
    @Option(names = {"--diffRoles"}, description = "Show differences in roles", hidden = true)
    private boolean diffRoles;
    
    @Option(names = {"--diffShardKeys"}, description = "Diff shard keys (use 'sync' to synchronize)", hidden = true)
    private String diffShardKeys;
    
    @Override
    public Integer call() throws Exception {
        // Handle legacy options if provided
        if (collCounts || compareDbMeta || compareCollectionUuids || diffRoles || diffShardKeys != null) {
            return parent.executeCompareCommand(this);
        }
        
        CommandLine commandLine = new CommandLine(this);
        String subcommands = String.join(", ", commandLine.getSubcommands().keySet());
        System.out.println("Please specify a compare subcommand: " + subcommands);
        System.out.println("Use 'compare --help' to see available options");
        return 1;
    }
    
    /**
     * Create configuration for subcommands to use
     */
    public SyncConfiguration createConfiguration() {
        return parent.createBaseConfiguration(atlasOptions);
    }
    
    // Getters for legacy support
    public AtlasOptionsMixin getAtlasOptions() { return atlasOptions; }
    public boolean isCollCounts() { return collCounts; }
    public boolean isCompareDbMeta() { return compareDbMeta; }
    public boolean isCompareCollectionUuids() { return compareCollectionUuids; }
    public boolean isDiffRoles() { return diffRoles; }
    public String getDiffShardKeys() { return diffShardKeys; }
    
    // These are now handled by subcommands but kept for backward compatibility
    public boolean isChunkCounts() { return false; }
    public boolean isCompareChunks() { return false; }
    public boolean isCompareChunksEquivalent() { return false; }
    public boolean isCompareAndMoveChunks() { return false; }
    public boolean isCompareIndexes() { return false; }
    public boolean isDiffUsers() { return false; }
}