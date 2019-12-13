package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoIterable;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;


public class MongoSync {
    
    protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);
    
    private final static String DROP_DEST_DBS = "dropDestDbs";
    
    private MongoSyncOptions mongoSyncOptions;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    
    private ExecutorService initialSyncExecutor;
    
    private void initialize() {
        sourceShardClient = new ShardClient("source", mongoSyncOptions.getSourceMongoUri());
        sourceShardClient.populateShardMongoClients();
        
        destShardClient = new ShardClient("dest", mongoSyncOptions.getDestMongoUri());
        destShardClient.populateShardMongoClients();
        
        populateCollectionsToMigrate();
        sourceShardClient.populateCollectionsMap(mongoSyncOptions.getNamespacesToMigrate());
    }
    
    // TODO - currently only does entire databases
    private void populateCollectionsToMigrate() {
        Set<String> namespacesToMigrate = new HashSet<String>();
        
//        for (String dbName : mongoSyncOptions.getDatabaseFilters()) {
//            
//            if (mongoSyncOptions.isDropDestDbs()) {
//                destShardClient.dropDatabase(dbName);
//            }
//            
//            MongoIterable<String> collectionNames = sourceShardClient.listCollectionNames(dbName);
//            for (String collectionName : collectionNames) {
//                namespacesToMigrate.add(new Namespace(dbName, collectionName).getNamespace());
//            }
//        }
        for (Namespace n : mongoSyncOptions.getNamespaceFilters()) {
            namespacesToMigrate.add(n.getNamespace());
        }
        mongoSyncOptions.setNamespacesToMigrate(namespacesToMigrate);
        logger.debug("namespacesToMigrate: " + namespacesToMigrate);
    }
    
    private void collectOplogLatestTimestamps() throws InterruptedException, ExecutionException {
        Collection<Callable<ShardTimestamp>> tasks = new ArrayList<>();
        for (String shardId : sourceShardClient.getShardsMap().keySet()) {
            tasks.add(new GetLatestOplogTimestampTask(shardId, sourceShardClient));
        }
        
        int numThreads = tasks.size();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<ShardTimestamp>> results;
        try {
            results = executor.invokeAll(tasks);
            for(Future<ShardTimestamp> result : results){
                ShardTimestamp shardTimestamp = result.get();
                sourceShardClient.getShardsMap().get(shardTimestamp.getShardName()).setSyncStartTimestamp(shardTimestamp);
                logger.debug(shardTimestamp.toString());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw(e);
        } finally {
            executor.shutdown();
        }
    }
    
    private void cloneCollection(Namespace ns) {
        Runnable worker = new DummyCloneWorker(ns, sourceShardClient, destShardClient, mongoSyncOptions);
        initialSyncExecutor.execute(worker);
        
    }
    
    private void initialSync() {
        initialSyncExecutor = Executors.newFixedThreadPool(mongoSyncOptions.getThreads());
        Set<String> namespaces = mongoSyncOptions.getNamespacesToMigrate();
        if (namespaces.isEmpty()) {
            
            MongoIterable<String> dbNames = sourceShardClient.listDatabaseNames();
            for (String dbName : dbNames) {
                
                if (! databasesBlacklist.contains(dbName)) {
                    MongoIterable<String> collectionNames = sourceShardClient.listCollectionNames(dbName);
                    for (String collectionName : collectionNames) {
                        if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
                            continue;
                        }
                        cloneCollection(new Namespace(dbName, collectionName));
                    }
                }
            }
            
        } else {
            for (String ns : namespaces) {
                cloneCollection(new Namespace(ns));
            }
        }

        initialSyncExecutor.shutdown();
        while (!initialSyncExecutor.isTerminated()) {
        }
        logger.debug("Initial sync complete");
    }
    
    private void tailOplogs() {
        Collection<Shard> shards = sourceShardClient.getShardsMap().values();
        ExecutorService executor = Executors.newFixedThreadPool(shards.size());
        
        for (Shard shard : shards) {
            Runnable worker = new OplogTailWorker(shard.getSyncStartTimestamp(), sourceShardClient, destShardClient, mongoSyncOptions);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        logger.debug("Finished all threads");
    }
    
    private void execute() {
        
        initialSync();
        
//        try {
//            collectOplogLatestTimestamps();
//            
//            //tailOplogs();
//        } catch (InterruptedException | ExecutionException e) {
//            logger.error("Error collecting latest oplog timestamps", e);
//            return;
//            // TODO exit?
//        }
        
        
    }
    
    
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("replayUtil", options);
        System.exit(-1);
    }
    
    @SuppressWarnings("static-access")
    protected static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
       
        options.addOption(OptionBuilder.withArgName("source cluster mongo uri").hasArg().withLongOpt("source").isRequired().create("s"));
        options.addOption(OptionBuilder.withArgName("destination cluster mongo uri").hasArg().withLongOpt("destination").isRequired().create("d"));
        options.addOption(OptionBuilder.withArgName("# threads").hasArgs().withLongOpt("threads").create("t"));
        options.addOption(OptionBuilder.withArgName("Namespace filter").hasArgs().withLongOpt("filter")
                .isRequired(false).create("f"));
        options.addOption(OptionBuilder.withArgName("Drop destination databases, but preserve config metadata")
                .withLongOpt(DROP_DEST_DBS).create(DROP_DEST_DBS));
        
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
    
    protected void parseArgs(String args[]) {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        this.mongoSyncOptions = new MongoSyncOptions();
        
        mongoSyncOptions.setSourceMongoUri(line.getOptionValue("s"));
        mongoSyncOptions.setDestMongoUri(line.getOptionValue("d"));

        
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            mongoSyncOptions.setThreads(threads);
        }
        
        mongoSyncOptions.setNamespaceFilters(line.getOptionValues("f"));
        mongoSyncOptions.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
        
    }
    
    
    
    public static void main(String args[]) throws Exception {

        MongoSync sync = new MongoSync();
        sync.parseArgs(args);
        sync.initialize();
        sync.execute();
    }

}
