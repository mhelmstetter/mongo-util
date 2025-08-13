package com.mongodb.shardsync.command.compare;

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.shardsync.command.CompareCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "chunks", description = "Compare chunks between source and destination")
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
        } else if (equivalent) {
            sync.compareChunksEquivalent();
        } else if (move) {
            sync.compareAndMoveChunks(true, false);
        } else {
            sync.compareChunks();
        }
        
        return 0;
    }
}