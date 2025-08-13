package com.mongodb.shardsync.command.drop;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.DropCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "databases", description = "Drop databases on destination cluster")
public class DropDatabasesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private DropCommand parent;
    
    @Option(names = {"--include-config"}, 
            description = "Also drop config metadata")
    private boolean includeConfig;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (includeConfig) {
            sync.dropDestinationDatabasesAndConfigMetadata();
        } else {
            sync.dropDestinationDatabases();
        }
        
        return 0;
    }
}