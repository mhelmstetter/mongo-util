package com.mongodb.shardsync.command.compare;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "chunks", 
         mixinStandardHelpOptions = true,
         description = "Compare chunks between source and destination")
public class CompareChunksCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private CompareCommand parent;
    
    @Option(names = {"--equivalent"}, 
            description = "Check for chunk equivalence")
    private boolean equivalent;
    
    @Option(names = {"--counts"}, 
            description = "Compare chunk counts only")
    private boolean counts;
    
    @Option(names = {"--move"}, 
            description = "Compare and move chunks if needed")
    private boolean move;
    
    @Override
    public Integer call() throws Exception {
        SyncConfiguration config = parent.createConfiguration();
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (counts) {
            sync.compareChunkCounts();
            return 0;
        } else if (equivalent) {
            boolean isEquivalent = sync.compareChunksEquivalent();
            return isEquivalent ? 0 : 1;  // Return 0 for success, 1 for failure
        } else if (move) {
            boolean success = sync.compareAndMoveChunks(true, false);
            return success ? 0 : 1;  // Return 0 for success, 1 for failure
        } else {
            boolean success = sync.compareChunks();
            return success ? 0 : 1;  // Return 0 for success, 1 for failure
        }
    }
}