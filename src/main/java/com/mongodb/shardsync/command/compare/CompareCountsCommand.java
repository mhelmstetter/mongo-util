package com.mongodb.shardsync.command.compare;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/**
 * Compare document counts between source and destination collections across all shards.
 * This helps identify data discrepancies between clusters.
 */
@Command(name = "counts", 
         description = "Compare document counts between source and destination collections")
public class CompareCountsCommand implements Callable<Integer> {
    
    private static final Logger logger = LoggerFactory.getLogger(CompareCountsCommand.class);
    
    @ParentCommand
    private CompareCommand parent;
    
    @Override
    public Integer call() throws Exception {
        logger.debug("Starting compare counts command");
        
        SyncConfiguration config = parent.createConfiguration();
        
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        boolean success = sync.compareShardCounts();
        
        return success ? 0 : 1;
    }
}