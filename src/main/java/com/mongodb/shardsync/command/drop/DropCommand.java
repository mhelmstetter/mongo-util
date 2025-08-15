package com.mongodb.shardsync.command;

import com.mongodb.shardsync.ShardConfigSyncApp;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.drop.*;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "drop", 
         description = "Drop databases, collections, or indexes",
         subcommands = {
             DropDatabasesCommand.class,
             DropUsersCommand.class,
             DropIndexesCommand.class
         })
public class DropCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @CommandLine.Mixin
    private AtlasOptionsMixin atlasOptions = new AtlasOptionsMixin();
    
    @Override
    public Integer call() throws Exception {
        System.out.println("Please specify a drop subcommand: databases, users, or indexes");
        System.out.println("Use --help to see available options");
        return 1;
    }
    
    /**
     * Create configuration for subcommands to use
     */
    public SyncConfiguration createConfiguration() {
        return parent.createBaseConfiguration(atlasOptions);
    }
    
    // Getters for compatibility
    public AtlasOptionsMixin getAtlasOptions() { return atlasOptions; }
    
    // Legacy getters that return false (these operations are now in subcommands)
    public boolean isDrop() { return false; }
    public boolean isDropDestDbs() { return false; }
    public boolean isDropDestDbsAndConfigMetadata() { return false; }
    public boolean isDropDestAtlasUsersAndRoles() { return false; }
    public boolean isDropIndexes() { return false; }
}