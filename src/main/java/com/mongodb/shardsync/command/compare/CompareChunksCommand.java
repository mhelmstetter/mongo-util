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
            description = "Check for chunk equivalence (default behavior)")
    private boolean equivalent;
    
    @Option(names = {"--strict"}, 
            description = "Use strict chunk comparison (mutually exclusive with --equivalent)")
    private boolean strict;
    
    @Option(names = {"--counts"}, 
            description = "Compare chunk counts only")
    private boolean counts;
    
    @Option(names = {"--move"},
            description = "Compare and move chunks if needed")
    private boolean move;

    @Option(names = {"--legacyCatalogMirror"},
            description = "Use legacy chunk comparison method (by default, uses CatalogVerifier)")
    private boolean legacyCatalogMirror;

    @Override
    public Integer call() throws Exception {
        // Validate mutually exclusive options
        if (equivalent && strict) {
            System.err.println("Error: --equivalent and --strict are mutually exclusive");
            return 1;
        }

        SyncConfiguration config = parent.createConfiguration();
        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();

        // If NOT using legacy catalog mirror, use the new CatalogVerifier methods
        if (!legacyCatalogMirror) {
            // New verification method - equivalent, strict, and move options don't apply
            if (equivalent || strict || move) {
                System.err.println("Warning: --equivalent, --strict, and --move options are ignored when using CatalogVerifier (default)");
                System.err.println("Use --legacyCatalogMirror if you need these options");
            }

            if (counts) {
                boolean success = sync.compareChunkCounts();
                return success ? 0 : 1;
            } else {
                // Use new CatalogVerifier method
                boolean success = sync.verifyCatalogMetadata();
                return success ? 0 : 1;
            }
        }

        // Legacy comparison methods
        if (counts) {
            boolean success = sync.compareChunkCounts();
            return success ? 0 : 1;
        } else if (move) {
            boolean success = sync.compareAndMoveChunks(true, false);
            return success ? 0 : 1;  // Return 0 for success, 1 for failure
        } else if (strict) {
            // Use strict comparison
            boolean success = sync.compareChunks();
            return success ? 0 : 1;  // Return 0 for success, 1 for failure
        } else {
            // Default behavior: equivalent comparison (whether --equivalent is specified or not)
            boolean isEquivalent = sync.compareChunksEquivalent();
            return isEquivalent ? 0 : 1;  // Return 0 for success, 1 for failure
        }
    }
}