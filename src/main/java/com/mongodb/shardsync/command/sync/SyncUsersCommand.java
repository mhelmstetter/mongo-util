package com.mongodb.shardsync.command.sync;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.SyncCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "users", description = "Synchronize users to destination cluster")
public class SyncUsersCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private SyncCommand parent;
    
    @Option(names = {"--input"}, 
            description = "Users input CSV file", 
            defaultValue = "usersInput.csv")
    private String usersInputCsv;
    
    @Option(names = {"--output"}, 
            description = "Users output CSV file", 
            defaultValue = "usersOutput.csv")
    private String usersOutputCsv;
    
    @Option(names = {"--test-auth"}, 
            description = "Test user authentications using credentials from input CSV")
    private boolean testAuth;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        config.setUsersInputCsv(usersInputCsv);
        config.setUsersOutputCsv(usersOutputCsv);
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (testAuth) {
            sync.testUsersAuth();
        } else {
            sync.syncUsers();
        }
        
        return 0;
    }
}