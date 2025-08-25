package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSyncApp;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.AtlasOptionsMixin;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "sync", 
         description = "Synchronize data between clusters",
         subcommands = {
             SyncMetadataCommand.class,
             SyncUsersCommand.class,
             SyncRolesCommand.class,
             SyncIndexesCommand.class,
             SyncShardingCommand.class
         })
public class SyncCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @CommandLine.Mixin
    private AtlasOptionsMixin atlasOptions = new AtlasOptionsMixin();

    @Override
    public Integer call() throws Exception {
        CommandLine commandLine = new CommandLine(this);
        String subcommands = String.join(", ", commandLine.getSubcommands().keySet());
        System.out.println("Please specify a sync subcommand: " + subcommands);
        System.out.println("Use 'sync --help' to see available options");
        return 1;
    }
    
    /**
     * Create configuration for subcommands to use
     */
    public SyncConfiguration createConfiguration() {
        return parent.createBaseConfiguration(atlasOptions);
    }

    // Getters for compatibility (can be removed if ShardConfigSyncApp is updated)
    public AtlasOptionsMixin getAtlasOptions() { return atlasOptions; }
}