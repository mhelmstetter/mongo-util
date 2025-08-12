package com.mongodb.shardsync.command;

import com.mongodb.shardsync.ShardConfigSyncApp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "sync", description = "Synchronize data between clusters")
public class SyncCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @CommandLine.Mixin
    private AtlasOptionsMixin atlasOptions = new AtlasOptionsMixin();

    @Option(names = {"--metadataLegacy"}, hidden = true, description = "Synchronize shard metadata, legacy method (slow)")
    private boolean syncMetadataLegacy;
    
    // This is equivalent to the "old" syncMetadataOptimized that uses megachunks but still
    // creates an equal number of chunks as the source
    @Option(names = {"--metadata"}, description = "Synchronize shard metadata")
    private boolean syncMetadata;

    
    @Option(names = {"--metadataOptimized"}, description = "Synchronize shard metadata, combining any adjacent chunks")
    private boolean syncMetadataOptimized;

    @Option(names = {"--indexes"}, description = "Copy indexes from source to dest")
    private boolean syncIndexes;

    @Option(names = {"--users"}, description = "Synchronize users to Atlas")
    private boolean syncUsers;

    @Option(names = {"--roles"}, description = "Synchronize roles to Atlas")
    private boolean syncRoles;

    @Option(names = {"--collation"}, description = "Collation to apply to index creation")
    private String collation;

    @Option(names = {"--extendTtl"}, description = "Extend TTL expiration (use with --indexes)")
    private boolean extendTtl;

    @Option(names = {"--ttlOnly"}, description = "Only sync TTL indexes (use with --indexes)")
    private boolean ttlOnly;

    @Option(names = {"--collModTtl"}, description = "collMod all ttl indexes on dest based on source")
    private boolean collModTtl;

    @Option(names = {"--usersInput"}, description = "users input csv")
    private String usersInputCsv;

    @Option(names = {"--usersOutput"}, description = "Users output CSV file")
    private String usersOutputCsv;
    
    @Option(names = {"--testUsersAuth"}, description = "Test user authentications using credentials from input csv")
    private boolean testUsersAuth;

    @Override
    public Integer call() throws Exception {
        return parent.executeSyncCommand(this);
    }

    // Getters for the parent class to access the fields
    public AtlasOptionsMixin getAtlasOptions() { return atlasOptions; }
    public boolean isSyncMetadataLegacy() { return syncMetadataLegacy; }
    public boolean isSyncMetadataOptimized() { return syncMetadataOptimized; }
    public boolean isSyncIndexes() { return syncIndexes; }
    public boolean isSyncUsers() { return syncUsers; }
    public boolean isSyncRoles() { return syncRoles; }
    public String getCollation() { return collation; }
    public boolean isExtendTtl() { return extendTtl; }
    public boolean isTtlOnly() { return ttlOnly; }
    public boolean isCollModTtl() { return collModTtl; }
    public String getUsersInputCsv() { return usersInputCsv; }
    public String getUsersOutputCsv() { return usersOutputCsv; }
    public boolean isTestUsersAuth() { return testUsersAuth; }
}