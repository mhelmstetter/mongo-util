package com.mongodb.mongosync;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
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
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoIterable;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;


public class MongoSync {
    
    protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);
    
    CodecRegistry registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
			fromProviders(PojoCodecProvider.builder().automatic(true).build()));

	DocumentCodec documentCodec = new DocumentCodec(registry);
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    
    private final static String SPLIT_CHUNKS = "splitChunks";
    private final static String DROP_DEST_DBS = "dropDestDbs";
    private final static String CLEAN_TIMESTAMPS = "cleanTs";
    private final static String MULTI_OPLOG_WORKER = "multiWorker";
    private final static String OPLOG_THREADS = "oplogThreads";
    private final static String OPLOG_QUEUE_SIZE = "oplogQueueSize";
    private final static String NAME = "name";
    
    private static Options options;
    private static CommandLine line;
    
    private MongoSyncOptions mongoSyncOptions;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private ExecutorService initialSyncExecutor;
    private ExecutorService oplogTailExecutor;
    
    private List<AbstractOplogTailWorker> oplogTailWorkers;
    
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
        stopSourceBalancer();
    }
    
    private void stopSourceBalancer() {

		logger.debug("stopSourceBalancer started");
		try {
			sourceShardClient.stopBalancer();
		} catch (MongoCommandException mce) {
			logger.error("Could not stop balancer on source shard: " + mce.getMessage());
		}
		
		logger.debug("stopSourceBalancer complete");
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
    	Collection<Callable<ShardTimestamp>> tasks = new ArrayList<>();
    	timestampFiles = new HashMap<>();
    	shardTimestamps = new HashMap<>();
    	
    	for (String shardId : sourceShardClient.getShardsMap().keySet()) {
    		
    		TimestampFile tsFile = new TimestampFile(shardId);
    		
    		timestampFiles.put(shardId, tsFile);
    		if (tsFile.exists() && ! mongoSyncOptions.isCleanTimestampFiles()) {
    			
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
                    logger.debug("GetLatestOplogTimestampTask result: {}", shardTimestamp.toString());
                   
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
                
                if (! ShardClient.excludedSystemDbs.contains(dbName)) {
                	
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
        
        oplogTailExecutor = Executors.newFixedThreadPool(shards.size());
        oplogTailWorkers = new ArrayList<>(shards.size());
        for (Shard shard : shards) {
        	createWorker(shard.getId());
        }
        
//        oplogTailExecutor = Executors.newFixedThreadPool(1);
//        oplogTailWorkers = new ArrayList<>(1);
//        createWorker(shards.iterator().next().getId());
        
        
        oplogTailExecutor.shutdown();
        while (!oplogTailExecutor.isTerminated()) {
        	try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
			}
        }
        logger.debug("tailOplogs shutdown");
        logger.debug("Finished all threads");
    }
    
    private void createWorker(String shardId) {
    	ShardTimestamp shardTimestamp = shardTimestamps.get(shardId);
    	TimestampFile timestampFile = timestampFiles.get(shardId);
    	
    	AbstractOplogTailWorker worker;
		try {
			//if (mongoSyncOptions.isUseMultiThreadedOplogTailWorkers()) {
			worker = new MultiBufferOplogTailWorker(shardTimestamp, timestampFile, sourceShardClient, destShardClient, mongoSyncOptions);
			oplogTailWorkers.add(worker);
			oplogTailExecutor.execute(worker);
		} catch (IOException e) {
			logger.error(String.format("Error creating OplogTailWorker", e));
		}
    }
    
    private void execute() {
        
    	if (mongoSyncOptions.isCleanTimestampFiles()) {
    		// TODO
    	}
    	
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
    
    private void shutdown() {
    	if (oplogTailExecutor != null) {
    		for (AbstractOplogTailWorker oplogTailWorker : oplogTailWorkers) {
    			oplogTailWorker.stop();
    		}
    		logger.debug("workers stopped");
    	}
    	
    }
    
    
    private void splitChunks() {
    	logger.debug("Starting splitChunks");
    	
    	for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {
    		String nsStr = (String) sourceColl.get("_id");
			Namespace ns = new Namespace(nsStr);
			if (ShardClient.excludedSystemDbs.contains(ns.getDatabaseName())) {
				continue;
			}
			List<Document> splitPoints = sourceShardClient.splitVector(ns, sourceColl);
			logger.debug("ns: {}, splitCount: {}", ns, splitPoints.size());
			
			for (Document split : splitPoints) {
				Document chunk = new Document();
				chunk.put("ns", nsStr);
				chunk.put("max", split);
				destShardClient.createChunk(chunk.toBsonDocument(BsonDocument.class, registry), false, false);
			}
    	}
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
        options.addOption(OptionBuilder.withArgName("source cluster mongo uri").hasArg().withLongOpt(SOURCE_URI).create("s"));
        options.addOption(OptionBuilder.withArgName("destination cluster mongo uri").hasArg().withLongOpt(DEST_URI).create("d"));
        options.addOption(OptionBuilder.withArgName("split destination chunks").withLongOpt(SPLIT_CHUNKS).create("x"));
        options.addOption(OptionBuilder.withArgName("# threads").hasArg().withLongOpt("threads").create("t"));
        options.addOption(OptionBuilder.withArgName("batch size").hasArg().withLongOpt("batchSize").create("b"));
        options.addOption(OptionBuilder.withArgName("Namespace filter").hasArgs().withLongOpt("filter").create("f"));
        options.addOption(OptionBuilder.withArgName("Drop destination databases, but preserve config metadata")
                .withLongOpt(DROP_DEST_DBS).create(DROP_DEST_DBS));
        options.addOption(OptionBuilder.withArgName("Cleanup and previous/old timestamp files")
                .withLongOpt(CLEAN_TIMESTAMPS).create(CLEAN_TIMESTAMPS));
        options.addOption(OptionBuilder.withArgName("Use multi-threaded oplog tail workers")
                .withLongOpt(MULTI_OPLOG_WORKER).create(MULTI_OPLOG_WORKER));
        options.addOption(OptionBuilder.withArgName("# oplog tailing threads (per shard)").hasArg()
                .withLongOpt(OPLOG_THREADS).create(OPLOG_THREADS));
        options.addOption(OptionBuilder.withArgName("oplog queue size (per shard)").hasArg()
                .withLongOpt(OPLOG_QUEUE_SIZE).create(OPLOG_QUEUE_SIZE));
        
        options.addOption(OptionBuilder.withArgName("name for this sync process")
                .withLongOpt(NAME).create(NAME));
        
        
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
    
    protected void parseArgs() {
        
        
        Configuration config = readProperties();
        
        this.mongoSyncOptions = new MongoSyncOptions();
        
        mongoSyncOptions.setSourceMongoUri(line.getOptionValue("s", config.getString(SOURCE_URI)));
        mongoSyncOptions.setDestMongoUri(line.getOptionValue("d", config.getString(DEST_URI)));
        

        
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            mongoSyncOptions.setThreads(threads);
        }
        
        String batchSizeStr = line.getOptionValue("b");
        if (batchSizeStr != null) {
            int batchSize = Integer.parseInt(batchSizeStr);
            mongoSyncOptions.setBatchSize(batchSize);
        }
        
        mongoSyncOptions.setNamespaceFilters(line.getOptionValues("f"));
        mongoSyncOptions.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
        mongoSyncOptions.setCleanTimestampFiles(line.hasOption(CLEAN_TIMESTAMPS));
        mongoSyncOptions.setUseMultiThreadedOplogTailWorkers(line.hasOption(MULTI_OPLOG_WORKER));
        
        String oplogThreadsStr = line.getOptionValue(OPLOG_THREADS);
        if (oplogThreadsStr != null) {
            int oplogThreads = Integer.parseInt(oplogThreadsStr);
            mongoSyncOptions.setOplogThreads(oplogThreads);
        }
        
        String oplogQueueSizeStr = line.getOptionValue(OPLOG_QUEUE_SIZE);
        if (oplogQueueSizeStr != null) {
            int oplogQueueSize = Integer.parseInt(oplogQueueSizeStr);
            mongoSyncOptions.setOplogQueueSize(oplogQueueSize);
        }
        
    }
    
    private static void addShutdownHook(MongoSync sync) {
    	Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                logger.debug("**** SHUTDOWN *****");
                sync.shutdown();
            }
        }));
    }
    
    
    
    public static void main(String args[]) throws Exception {
    	initializeAndParseCommandLineOptions(args);
    	
        MongoSync sync = new MongoSync();
        sync.parseArgs();
        sync.initialize();
        if (line.hasOption(SPLIT_CHUNKS)) {
        	sync.splitChunks();
        } else {
        	addShutdownHook(sync);
        	sync.execute();
        }
        
        
    }

}
