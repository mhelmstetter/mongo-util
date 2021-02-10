package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoIterable;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;


public class MongoSync {
    
    protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    
    private final static String DROP_DEST_DBS = "dropDestDbs";
    
    private static Options options;
    private static CommandLine line;
    
    private MongoSyncOptions mongoSyncOptions;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    
    private ExecutorService initialSyncExecutor;
    
    private Map<String, TimestampFile> timestampFiles;
    private Map<String, ShardTimestamp> shardTimestamps;
    
    private boolean doInitialSync = true;
    
    
    private void initialize() throws IOException {
        sourceShardClient = new ShardClient("source", mongoSyncOptions.getSourceMongoUri());
        sourceShardClient.init();
        sourceShardClient.populateShardMongoClients();
        
        destShardClient = new ShardClient("dest", mongoSyncOptions.getDestMongoUri());
        destShardClient.init();
        destShardClient.populateShardMongoClients();
        
        populateCollectionsToMigrate();
        sourceShardClient.populateCollectionsMap(mongoSyncOptions.getNamespacesToMigrate());
        
    }
    
    private void stopBalancers() {

		logger.debug("stopBalancers started");
		try {
			sourceShardClient.stopBalancer();
		} catch (MongoCommandException mce) {
			logger.error("Could not stop balancer on source shard: " + mce.getMessage());
		}
		
		
		try {
			destShardClient.stopBalancer();
		} catch (MongoCommandException mce) {
			logger.error("Could not stop balancer on dest shard: " + mce.getMessage());
		}
		
		logger.debug("stopBalancers complete");
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
    
    private void collectOplogLatestTimestamps() throws InterruptedException, ExecutionException, IOException {
    	Set<String> sourceShards = sourceShardClient.getShardsMap().keySet();
    	Collection<Callable<ShardTimestamp>> tasks = new ArrayList<>();
    	timestampFiles = new HashMap<>();
    	shardTimestamps = new HashMap<>();
    	
    	for (String shardId : sourceShardClient.getShardsMap().keySet()) {
    		
    		TimestampFile tsFile = new TimestampFile(shardId);
    		
    		timestampFiles.put(shardId, tsFile);
    		if (tsFile.exists()) {
    			doInitialSync = false;
    			try {
					ShardTimestamp st = tsFile.getShardTimestamp();
					logger.debug(String.format("timestamp file %s exists: %s", tsFile, st));
					shardTimestamps.put(shardId, st);
				} catch (IOException e) {
					logger.error(String.format("Error reading timestamp file %s", tsFile), e);
					throw e;
				}
    		} else {
    			tasks.add(new GetLatestOplogTimestampTask(shardId, sourceShardClient));
    		}
    	}
    	
        
    	if (tasks.size() > 0) {
    		int numThreads = tasks.size();
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            List<Future<ShardTimestamp>> results;
            try {
                results = executor.invokeAll(tasks);
                for(Future<ShardTimestamp> result : results){
                    ShardTimestamp shardTimestamp = result.get();
                    shardTimestamps.put(shardTimestamp.getShardName(), shardTimestamp);
                    sourceShardClient.getShardsMap().get(shardTimestamp.getShardName()).setSyncStartTimestamp(shardTimestamp);
                    logger.debug(shardTimestamp.toString());
                   
                    TimestampFile timestampFile = timestampFiles.get(shardTimestamp.getShardName());
                    timestampFile.update(shardTimestamp);
                }
            } catch (InterruptedException | ExecutionException e) {
                throw(e);
            } finally {
                executor.shutdown();
            }
    	}
        
    }
    
    private void cloneCollection(Namespace ns) {
        Runnable worker = new CollectionCloneWorker(ns, sourceShardClient, destShardClient, mongoSyncOptions);
        initialSyncExecutor.execute(worker);
        
    }
    
    private void initialSync() {
    	logger.debug("Starting initial sync");
        initialSyncExecutor = Executors.newFixedThreadPool(mongoSyncOptions.getThreads());
        Set<String> namespaces = mongoSyncOptions.getNamespacesToMigrate();
        if (namespaces.isEmpty()) {
            
            MongoIterable<String> dbNames = sourceShardClient.listDatabaseNames();
            for (String dbName : dbNames) {
                
                if (! databasesBlacklist.contains(dbName)) {
                	
                	if (mongoSyncOptions.isDropDestDbs()) {
                		destShardClient.dropDatabase(dbName);
                	}
                	
                	
                	List<String> collectionNames = new ArrayList<>();
                    sourceShardClient.listCollectionNames(dbName).into(collectionNames);
                    logger.debug("{}: collection count: {}", dbName, collectionNames.size());
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
        
        logger.debug("tailOplogs: {} shards", shards.size());
        
        ExecutorService executor = Executors.newFixedThreadPool(shards.size());
        
        
        for (Shard shard : shards) {
        	ShardTimestamp shardTimestamp = shardTimestamps.get(shard.getId());
        	TimestampFile timestampFile = timestampFiles.get(shard.getId());
        	
            Runnable worker;
			try {
				worker = new OplogTailWorker(shardTimestamp, timestampFile, sourceShardClient, destShardClient, mongoSyncOptions);
				executor.execute(worker);
			} catch (IOException e) {
				logger.error(String.format("Error creating OplogTailWorker", e));
			}
            
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        logger.debug("Finished all threads");
    }
    
    private void execute() {
        
    	stopBalancers();
        try {
            collectOplogLatestTimestamps();
        } catch (IOException | InterruptedException | ExecutionException e) {
            logger.error("Error collecting latest oplog timestamps", e);
            return;
            // TODO exit?
        }
        if (doInitialSync) {
        	initialSync();
        } else {
        	logger.debug("Skippping initial sync, timestamp file(s) exist");
        }
        
        tailOplogs();
        
    }
    
    
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("replayUtil", options);
        System.exit(-1);
    }
    
    @SuppressWarnings("static-access")
    protected static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
       
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArg().withLongOpt("config").create("c"));
        options.addOption(OptionBuilder.withArgName("source cluster mongo uri").hasArg().withLongOpt("source").create("s"));
        options.addOption(OptionBuilder.withArgName("destination cluster mongo uri").hasArg().withLongOpt("destination").create("d"));
        options.addOption(OptionBuilder.withArgName("# threads").hasArg().withLongOpt("threads").create("t"));
        options.addOption(OptionBuilder.withArgName("Namespace filter").hasArgs().withLongOpt("filter")
                .isRequired(false).create("f"));
        options.addOption(OptionBuilder.withArgName("Drop destination databases, but preserve config metadata")
                .withLongOpt(DROP_DEST_DBS).create(DROP_DEST_DBS));
        
        CommandLineParser parser = new GnuParser();
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
    
    private static Configuration readProperties() {
    	Configurations configs = new Configurations();
    	Configuration defaultConfig = new PropertiesConfiguration();
    	
        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else {
            propsFile = new File("shard-sync.properties");
            if (! propsFile.exists()) {
                logger.warn("Default config file shard-sync.properties not found, using command line options only");
                return defaultConfig;
            }
        }
        
        try {
			Configuration config = configs.properties(propsFile);
			return config;
		} catch (ConfigurationException e) {
			logger.error("Error loading properties file: " + propsFile, e);
		}
        return defaultConfig;
    }
    
    protected void parseArgs(String args[]) {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        Configuration config = readProperties();
        
        this.mongoSyncOptions = new MongoSyncOptions();
        
        mongoSyncOptions.setSourceMongoUri(line.getOptionValue("s", config.getString(SOURCE_URI)));
        mongoSyncOptions.setDestMongoUri(line.getOptionValue("d", config.getString(DEST_URI)));
        

        
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
