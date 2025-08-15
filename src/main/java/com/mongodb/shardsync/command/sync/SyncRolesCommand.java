package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.concurrent.Callable;

@Command(name = "roles", description = "Synchronize roles to destination cluster")
public class SyncRolesCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private SyncCommand parent;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        sync.syncRoles();
        
        return 0;
    }
}