package com.mongodb.shardsync;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardConfigSyncApp {
    
    private static Logger logger = LoggerFactory.getLogger(ShardConfigSyncApp.class);
    
    private static Options options;
    private static CommandLine line;
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    private final static String MONGOMIRROR_BINARY = "mongomirrorBinary";

    private final static String DROP_DEST_DBS = "dropDestDbs";
    private final static String DROP_DEST_DBS_AND_CONFIG_METADATA = "dropDestDbsAndConfigMeta";
    private final static String NON_PRIVILEGED = "nonPrivileged";
    private final static String PRESERVE_UUIDS = "preserveUUIDs";
    private final static String TAIL_ONLY = "tailOnly";
    private final static String COMPRESSORS = "compressors";
    
    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    private final static String FLUSH_ROUTER = "flushRouter";
    private final static String SYNC_METADATA = "syncMetadata";
    private final static String COMPARE_CHUNKS = "compareChunks";
    private final static String COMPARE_COLLECTION_UUIDS = "compareCollectionUuids";
    
    private final static String COMPARE_AND_MOVE_CHUNKS = "compareAndMoveChunks";
    private final static String MONGO_MIRROR = "mongomirror";
    private final static String SHARD_COLLECTIONS = "shardCollections";
    private final static String CLEANUP_ORPHANS = "cleanupOrphans";
    private final static String CLEANUP_ORPHANS_SLEEP = "cleanupOrphansSleep";
    private final static String CLEANUP_ORPHANS_DEST = "cleanupOrphansDest";
    
    private final static String SSL_ALLOW_INVALID_HOSTNAMES = "sslAllowInvalidHostnames";
    private final static String SSL_ALLOW_INVALID_CERTS = "sslAllowInvalidCertificates";

    private static final String SHARD_MAP = "shardMap";
    
    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArgs().withLongOpt("config")
                .isRequired(false).create("c"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt(SOURCE_URI)
                .isRequired(false).create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArgs().withLongOpt(DEST_URI)
                .isRequired(false).create("d"));
        options.addOption(OptionBuilder.withArgName("Drop destination databases, but preserve config metadata")
                .withLongOpt(DROP_DEST_DBS).create(DROP_DEST_DBS));
        options.addOption(OptionBuilder.withArgName("Drop destination databases AND config metadata (collections, databases, chunks)")
                .withLongOpt(DROP_DEST_DBS_AND_CONFIG_METADATA).create(DROP_DEST_DBS_AND_CONFIG_METADATA));
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
        options.addOption(OptionBuilder.withArgName("Compare all shard chunks, move any misplaced chunks")
                .withLongOpt(COMPARE_AND_MOVE_CHUNKS).create(COMPARE_AND_MOVE_CHUNKS));
        options.addOption(OptionBuilder.withArgName("Compare all collection UUIDs")
                .withLongOpt(COMPARE_COLLECTION_UUIDS).create(COMPARE_COLLECTION_UUIDS));
        
        options.addOption(OptionBuilder.withArgName("Synchronize shard metadata")
                .withLongOpt(SYNC_METADATA).create(SYNC_METADATA));
        options.addOption(OptionBuilder.withArgName("Cleanup source orphans")
                .withLongOpt(CLEANUP_ORPHANS).create(CLEANUP_ORPHANS));
        options.addOption(OptionBuilder.withArgName("Cleanup destination orphans")
                .withLongOpt(CLEANUP_ORPHANS_DEST).create(CLEANUP_ORPHANS_DEST));
        
        options.addOption(OptionBuilder.withArgName("Shard destination collections")
                .withLongOpt(SHARD_COLLECTIONS).create(SHARD_COLLECTIONS));
        
        options.addOption(OptionBuilder.withArgName("ssl allow invalid hostnames")
                .withLongOpt(SSL_ALLOW_INVALID_HOSTNAMES).create(SSL_ALLOW_INVALID_HOSTNAMES));
        options.addOption(OptionBuilder.withArgName("ssl allow invalid certificates")
                .withLongOpt(SSL_ALLOW_INVALID_CERTS).create(SSL_ALLOW_INVALID_CERTS));
        
        options.addOption(OptionBuilder.withArgName("preserve UUIDs")
                .withLongOpt(PRESERVE_UUIDS).create(PRESERVE_UUIDS));
        options.addOption(OptionBuilder.withArgName("mongomirror tail only")
                .withLongOpt(TAIL_ONLY).create(TAIL_ONLY));
        options.addOption(OptionBuilder.withArgName("mongomirror compressors").hasArg()
                .withLongOpt(COMPRESSORS).create("z"));
        
        options.addOption(OptionBuilder.withArgName("cleanup orphans sleep millis").hasArg()
                .withLongOpt(CLEANUP_ORPHANS_SLEEP).create(CLEANUP_ORPHANS_SLEEP));
        
        options.addOption(OptionBuilder.withArgName("Execute mongomirror(s)")
                .withLongOpt(MONGO_MIRROR).create(MONGO_MIRROR));
        options.addOption(OptionBuilder.withArgName("Namespace filter").hasArgs().withLongOpt("filter")
                .isRequired(false).create("f"));
        options.addOption(OptionBuilder.withArgName("full path to mongomirror binary").hasArgs().withLongOpt(MONGOMIRROR_BINARY)
                .isRequired(false).create("p"));
        options.addOption(OptionBuilder.withArgName("Shard mapping").hasArgs().withLongOpt(SHARD_MAP)
                .isRequired(false).create("m"));
        
        options.addOption(OptionBuilder.withArgName("Diff chunks").hasArgs().withLongOpt("diffChunks")
                .isRequired(false).create("z"));
        
        options.addOption(OptionBuilder.withArgName("writeConcern").hasArg().withLongOpt("writeConcern")
                .isRequired(false).create("w"));
        
        options.addOption(OptionBuilder.withArgName("Sleep millis").hasArg().withLongOpt("sleepMillis")
                .isRequired(false).create("x"));
        
        options.addOption(OptionBuilder.withArgName("numParallelCollections").hasArg().withLongOpt("numParallelCollections")
                .isRequired(false).create("y"));
        
        options.addOption(OptionBuilder.withArgName("shardToRs").withLongOpt("shardToRs")
                .isRequired(false).create("r"));
        
        options.addOption(OptionBuilder.withArgName("diffShardKeys [sync|diff]").withLongOpt("diffShardKeys")
                .isRequired(false).hasArg().create("k"));
        
        

        CommandLineParser parser = new GnuParser();
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit();
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit();
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit();
        }

        return line;
    }

    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("shardSync", options);
        System.exit(-1);
    }
    
    private static Properties readProperties() {
        Properties prop = new Properties();
        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else {
            propsFile = new File("shard-sync.properties");
            if (! propsFile.exists()) {
                logger.debug("Default config file shard-sync.properties not found, using command line options only");
                return prop;
            }
        }
        
        try (InputStream input = new FileInputStream(propsFile)) {
            prop.load(input);
        } catch (IOException ioe) {
            logger.error("Error loading properties file: " + propsFile, ioe);
        }
        return prop;
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        Properties configFileProps = readProperties();
        
        ShardConfigSync sync = new ShardConfigSync();
        sync.setSourceClusterUri(line.getOptionValue("s", configFileProps.getProperty(SOURCE_URI)));
        sync.setDestClusterUri(line.getOptionValue("d", configFileProps.getProperty(DEST_URI)));
        
        if (sync.getSourceClusterUri() == null || sync.getDestClusterUri() == null) {
            System.out.println("source and dest options required");
            printHelpAndExit();
        }
        
        String shardMaps = configFileProps.getProperty(SHARD_MAP);
        if (shardMaps != null) {
            sync.setShardMappings(shardMaps.split(","));
        } else {
            sync.setShardMappings(line.getOptionValues("m"));
        }
        
        sync.setNamespaceFilters(line.getOptionValues("f"));
        
        sync.setNonPrivilegedMode(line.hasOption(NON_PRIVILEGED));
        sync.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
        sync.setDropDestDbsAndConfigMetadata(line.hasOption(DROP_DEST_DBS_AND_CONFIG_METADATA));
        sync.setSleepMillis(line.getOptionValue("x"));
        sync.setNumParallelCollections(line.getOptionValue("y"));
        sync.setWriteConcern(line.getOptionValue("w"));
        
        sync.setSslAllowInvalidCertificates(line.hasOption(SSL_ALLOW_INVALID_CERTS));
        sync.setSslAllowInvalidHostnames(line.hasOption(SSL_ALLOW_INVALID_HOSTNAMES));
        
        sync.initializeShardMappings();
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
        } else if (line.hasOption(COMPARE_AND_MOVE_CHUNKS)) {
            actionFound = true;
            sync.compareAndMoveChunks(true);
        } else if (line.hasOption(COMPARE_COLLECTION_UUIDS)) {
        	actionFound = true;
        	sync.compareCollectionUuids();
        } else if (line.hasOption(SYNC_METADATA)) {
            actionFound = true;
            sync.migrateMetadata();
        }  else if (line.hasOption(SHARD_COLLECTIONS)) {
            actionFound = true;
            sync.enableDestinationSharding();
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
            sync.setCleanupOrphansSleepMillis(line.getOptionValue(CLEANUP_ORPHANS_SLEEP));
            sync.cleanupOrphans();
        }  else if (line.hasOption(CLEANUP_ORPHANS_DEST)) {
            actionFound = true;
            sync.cleanupOrphansDest();
        } else if (line.hasOption(DROP_DEST_DBS)) {
            actionFound = true;
            sync.dropDestinationDatabases();
        } else if (line.hasOption(DROP_DEST_DBS_AND_CONFIG_METADATA)) {
            actionFound = true;
            sync.dropDestinationDatabasesAndConfigMetadata();
        }
        // 
        
        // MONGOMIRROR_BINARY
        if (line.hasOption(MONGO_MIRROR)) {
            actionFound = true;
            String mongoMirrorPath = line.getOptionValue("p", configFileProps.getProperty(MONGOMIRROR_BINARY));
            
            boolean tailOnly = line.hasOption(TAIL_ONLY);
            boolean preserveUUIDs = line.hasOption(PRESERVE_UUIDS);
            
            if (line.hasOption(COMPRESSORS)) {
                sync.setCompressors(line.getOptionValue("c"));
            }
            
            if (mongoMirrorPath == null) {
                System.out.println("mongomirrorPath required");
                printHelpAndExit();
            }
            sync.setMongomirrorBinary(mongoMirrorPath);
            sync.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
            sync.setPreserveUUIDs(preserveUUIDs);
            sync.setTailOnly(tailOnly);
            sync.mongomirror();
        }
        
        if (line.hasOption("r")) {
            actionFound = true;
            if (!line.hasOption("p")) {
                System.out.println("mongomirrorPath required");
                printHelpAndExit();
            }
            sync.setMongomirrorBinary(line.getOptionValue("p"));
            sync.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
            sync.shardToRs();
        }
        
        if (! actionFound) {
            System.out.println("Missing action");
            printHelpAndExit();
        }
        
        // String[] fileNames = line.getOptionValues("f");
        // client.setEndpointUrl(line.getOptionValue("u"));

    }

}
