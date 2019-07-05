package com.mongodb.shardsync;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class ShardConfigSyncApp {
    
    private static Options options;

    private final static String DROP_DEST = "dropDestinationCollectionsIfExisting";
    private final static String NON_PRIVILEGED = "nonPrivileged";
    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    private final static String FLUSH_ROUTER = "flushRouter";
    private final static String SYNC_METADATA = "syncMetadata";
    private final static String COMPARE_CHUNKS = "compareChunks";
    private final static String MONGO_MIRROR = "mongomirror";
    private final static String SHARD_COLLECTIONS = "shardCollections";
    private final static String CLEANUP_ORPHANS = "cleanupOrphans";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source")
                .isRequired(true).create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArgs().withLongOpt("dest")
                .isRequired(true).create("d"));
        options.addOption(OptionBuilder.withArgName("Drop destination collections if existing")
                .withLongOpt(DROP_DEST).create(DROP_DEST));
        options.addOption(OptionBuilder.withArgName("Non-privileged mode, create chunks using splitChunk")
                .withLongOpt(NON_PRIVILEGED).create(NON_PRIVILEGED));
        options.addOption(OptionBuilder.withArgName("Compare counts only (do not sync/migrate)")
                .withLongOpt(COLL_COUNTS).create(COLL_COUNTS));
        options.addOption(OptionBuilder.withArgName("Show chunk counts when collection counts differ")
                .withLongOpt(CHUNK_COUNTS).create(CHUNK_COUNTS));
        options.addOption(OptionBuilder.withArgName("Flush router config on all mongos (do not sync/migrate)")
                .withLongOpt(FLUSH_ROUTER).create(FLUSH_ROUTER));
        options.addOption(OptionBuilder.withArgName("Compare all shard chunks (do not sync/migrate)")
                .withLongOpt(COMPARE_CHUNKS).create(COMPARE_CHUNKS));
        options.addOption(OptionBuilder.withArgName("Synchronize shard metadata")
                .withLongOpt(SYNC_METADATA).create(SYNC_METADATA));
        options.addOption(OptionBuilder.withArgName("Cleanup source orphans")
                .withLongOpt(CLEANUP_ORPHANS).create(CLEANUP_ORPHANS));
        
        options.addOption(OptionBuilder.withArgName("Shard destination collections")
                .withLongOpt(SHARD_COLLECTIONS).create(SHARD_COLLECTIONS));
        
        
        
        options.addOption(OptionBuilder.withArgName("Execute mongomirror(s)")
                .withLongOpt(MONGO_MIRROR).create(MONGO_MIRROR));
        options.addOption(OptionBuilder.withArgName("Namespace filter").hasArgs().withLongOpt("filter")
                .isRequired(false).create("f"));
        options.addOption(OptionBuilder.withArgName("full path to mongomirror binary").hasArgs().withLongOpt("mongomirrorBinary")
                .isRequired(false).create("p"));
        options.addOption(OptionBuilder.withArgName("Shard mapping").hasArgs().withLongOpt("shardMap")
                .isRequired(false).create("m"));
        
        options.addOption(OptionBuilder.withArgName("Diff chunks").hasArgs().withLongOpt("diffChunks")
                .isRequired(false).create("z"));
        
        options.addOption(OptionBuilder.withArgName("Sleep millis").hasArg().withLongOpt("sleepMillis")
                .isRequired(false).create("x"));
        
        options.addOption(OptionBuilder.withArgName("numParallelCollections").hasArg().withLongOpt("numParallelCollections")
                .isRequired(false).create("y"));
        
        options.addOption(OptionBuilder.withArgName("shardToRs").withLongOpt("shardToRs")
                .isRequired(false).create("r"));
        
        options.addOption(OptionBuilder.withArgName("diffShardKeys [sync|diff]").withLongOpt("diffShardKeys")
                .isRequired(false).hasArg().create("k"));
        
        

        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit(options);
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit(options);
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit(options);
        }

        return line;
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        ShardConfigSync sync = new ShardConfigSync();
        sync.setSourceClusterUri(line.getOptionValue("s"));
        sync.setDestClusterUri(line.getOptionValue("d"));
        sync.setNamespaceFilters(line.getOptionValues("f"));
        sync.setShardMappings(line.getOptionValues("m"));
        sync.setNonPrivilegedMode(line.hasOption(NON_PRIVILEGED));
        sync.setDropDestinationCollectionsIfExisting(line.hasOption(DROP_DEST));
        sync.setSleepMillis(line.getOptionValue("x"));
        sync.setNumParallelCollections(line.getOptionValue("y"));
        
        sync.init();
        boolean actionFound = false;
        if (line.hasOption(COLL_COUNTS)) {
            actionFound = true;
            sync.setDoChunkCounts(line.hasOption(CHUNK_COUNTS));
            sync.compareShardCounts();
        } else if (line.hasOption(FLUSH_ROUTER)) {
            actionFound = true;
            sync.flushRouterConfig();
        } else if (line.hasOption(COMPARE_CHUNKS)) {
            actionFound = true;
            sync.compareChunks();
        } else if (line.hasOption(SYNC_METADATA)) {
            actionFound = true;
            sync.migrateMetadata();
        }  else if (line.hasOption(SHARD_COLLECTIONS)) {
            actionFound = true;
            sync.doSharding();
        }  else if (line.hasOption("z")) {
            actionFound = true;
            sync.diffChunks(line.getOptionValue("z"));
        } else if (line.hasOption("k")) {
            String opt = line.getOptionValue("k");
            boolean doSync = opt.equals("sync");
            actionFound = true;
            sync.diffShardedCollections(doSync);
        }  else if (line.hasOption(CLEANUP_ORPHANS)) {
            actionFound = true;
            sync.cleanupOrphans();
        }
        
        if (line.hasOption(MONGO_MIRROR)) {
            actionFound = true;
            if (!line.hasOption("p")) {
                System.out.println("mongomirrorPath required");
                printHelpAndExit(options);
            }
            sync.setMongomirrorBinary(line.getOptionValue("p"));
            sync.setDropDestinationCollectionsIfExisting(line.hasOption(DROP_DEST));
            sync.mongomirror();
        }
        
        if (line.hasOption("r")) {
            actionFound = true;
            if (!line.hasOption("p")) {
                System.out.println("mongomirrorPath required");
                printHelpAndExit(options);
            }
            sync.setMongomirrorBinary(line.getOptionValue("p"));
            sync.setDropDestinationCollectionsIfExisting(line.hasOption(DROP_DEST));
            sync.shardToRs();
        }
        
        if (! actionFound) {
            System.out.println("Missing action");
            printHelpAndExit(options);
        }
        
        // String[] fileNames = line.getOptionValues("f");
        // client.setEndpointUrl(line.getOptionValue("u"));

    }

}
