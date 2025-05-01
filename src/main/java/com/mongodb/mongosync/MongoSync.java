package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoDatabase;
import com.mongodb.corruptutil.DupeUtil;
import com.mongodb.dbhash.DbHashUtil;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.util.ProcessUtils;
import com.mongodb.util.ProcessUtils.ProcessInfo;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "mongosync", mixinStandardHelpOptions = true, version = "mongosync 0.1", description = "mongosync runner", defaultValueProvider = PropertiesDefaultProvider.class)
public class MongoSync implements Callable<Integer>, MongoSyncPauseListener {

	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);

	@Option(names = { "--config",
			"-c" }, description = "config file", required = false, defaultValue = "mongosync.properties")
	private File configFile;

	@Option(names = { "--logDir" }, description = "log path", required = false)
	private File logDir;

	@Option(names = {
			"--loadLevel" }, description = "mongosync parallelism: between 1 (least parallel) to 4 (most parallel) (default: 3)", required = false, defaultValue = "3")
	private int loadLevel;

	@Option(names = { "--source" }, description = "source mongodb uri connection string", required = false)
	private String sourceUri;

	@Option(names = { "--dest" }, description = "destination mongodb uri connection string", required = false)
	private String destUri;

	@Option(names = { "--mongosyncBinary" }, description = "path to mongosync binary", required = false)
	private File mongosyncBinary;

	@Option(names = { "--buildIndexes" }, description = "Build indexes on target", required = false)
	private boolean buildIndexes = true;
	
	@Option(names = { "--dupeCheck" }, description = "Check for duplicate _ids", required = false)
	private boolean dupeCheck = true;

	@Option(names = { "--drop" }, description = "Drop target db and mongosync internal db", required = false)
	private boolean drop = false;

	@Option(names = { "--includeNamespaces" }, description = "Namespaces to include", required = false)
	private Set<String> includeNamespaces;

	@Option(names = { "--shardMap" }, description = "Shard map, ex: shA|sh0,shB|sh1", required = false)
	private String shardMap;

	@Option(names = { "--wiredTigerConfigString" }, description = "WiredTiger config string", required = false)
	private String wiredTigerConfigString;
	
	@Option(names = { "--dupeCheckTreads" }, description = "# threads per collection to use for duplicate _id checking", required = false, defaultValue = "4")
	private int dupeCheckThreads;
	
	@Option(names = { "--targetShards" }, description = "Target shards to distribute chunks to", required = false)
    private Set<String> targetShards;

	private ShardConfigSync shardConfigSync;
	private SyncConfiguration shardConfigSyncConfig;
	private ChunkManager chunkManager;
	private ShardClient sourceShardClient;
	private ShardClient destShardClient;
	private DupeUtil dupeUtil;

	List<MongoSyncRunner> mongosyncRunners;

	private AtomicInteger mongosyncRunnersPausedCount = new AtomicInteger(0);

	private void initialize() throws IOException {

		shardConfigSyncConfig = new SyncConfiguration();
		shardConfigSyncConfig.setSourceClusterUri(sourceUri);
		shardConfigSyncConfig.setDestClusterUri(destUri);
		if (includeNamespaces != null) {
			shardConfigSyncConfig.setNamespaceFilters(includeNamespaces.toArray(new String[0]));
		}
		
		if (shardMap != null) {
			shardConfigSyncConfig.setShardMap(shardMap.split(","));
		}

		if (wiredTigerConfigString != null) {
			shardConfigSyncConfig.setWiredTigerConfigString(wiredTigerConfigString);
		}

		chunkManager = new ChunkManager(shardConfigSyncConfig);
		chunkManager.initalize();
		this.sourceShardClient = shardConfigSyncConfig.getSourceShardClient();
		this.destShardClient = shardConfigSyncConfig.getDestShardClient();

		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap(includeNamespaces);
		destShardClient.populateShardMongoClients();
		destShardClient.stopBalancer();

		shardConfigSync = new ShardConfigSync(shardConfigSyncConfig);
		shardConfigSync.setChunkManager(chunkManager);
		shardConfigSync.initialize();

		if (drop) {
			Set<String> includes = shardConfigSyncConfig.getIncludeDatabasesAll();
			if (includes.isEmpty()) {
				shardConfigSync.dropDestinationDatabasesAndConfigMetadata();
			} else {
				destShardClient.dropDatabasesAndConfigMetadata(includes);
			}
			
			MongoDatabase msyncInternal = destShardClient.getMongoClient()
					.getDatabase("mongosync_reserved_for_internal_use");
			if (msyncInternal != null) {
				msyncInternal.drop();
			}
		}

		mongosyncRunners = new ArrayList<>(sourceShardClient.getShardsMap().size());

		if (logDir == null) {
			logDir = new File(".");
		}
		
		dupeUtil = new DupeUtil(sourceUri, destUri, "dupeArchive", null);
		dupeUtil.setThreads(dupeCheckThreads);
		dupeUtil.addFilters(includeNamespaces.toArray(new String[0]));

	}

	@Override
	public Integer call() throws Exception {
		initialize();
		
		if (dupeCheck) {
			long dupeCount = dupeUtil.run();
			
			if (dupeCount > 0) {
				//TODO
			}
		}

		List<Namespace> includes = null;
		if (includeNamespaces != null && !includeNamespaces.isEmpty()) {
			includes = new ArrayList<>();
			for (String ns : includeNamespaces) {
				includes.add(new Namespace(ns));
			}
		}

		List<ProcessInfo> mongosyncsRunningAtStart = ProcessUtils.killAllProcesses("mongosync");
		for (ProcessInfo p : mongosyncsRunningAtStart) {
			logger.warn("Found mongosync already running at start, process has been killed: {}", p);
		}

		int port = 27000;
		int i = 0;
		for (Shard source : sourceShardClient.getShardsMap().values()) {

			MongoSyncRunner mongosync = new MongoSyncRunner(source.getId(), this);
			mongosyncRunners.add(mongosync);
			mongosync.setSourceUri(sourceUri);
			mongosync.setDestinationUri(destUri);
			mongosync.setMongosyncBinary(mongosyncBinary);
			mongosync.setPort(port++);
			mongosync.setLoadLevel(loadLevel);
			mongosync.setBuildIndexes(buildIndexes);
			mongosync.setLogDir(logDir);
			mongosync.setIncludeNamespaces(includes);
			if (i == 0) {
				mongosync.setCoordinator(true);
			}
			
			String destShardId = chunkManager.getShardMapping(source.getId());
			Shard dest = destShardClient.getShardsMap().get(destShardId);
			logger.debug(String.format("Creating MongoSyncRunner for %s ==> %s", source.getId(), dest.getId()));
			mongosync.initialize();
			
			i++;
		}

		Thread.sleep(5000);

		// Start the first one (coordinator)
		MongoSyncRunner coordinator = mongosyncRunners.get(0);
		coordinator.start();
		

		while (true) {
			Thread.sleep(30000);
			int completeCount = 0;
			for (MongoSyncRunner mongosync : mongosyncRunners) {
//				status = mongosync.getStatus();
//				if (status != null) {
//					logger.debug("mongosync {}: status {}", mongosync.getId(), status);
//				}
				if (mongosync.isComplete()) {
					completeCount++;
				}
			}
			if (completeCount == mongosyncRunners.size()) {
				logger.debug("all done, exiting");
				break;
			}
		}

		DbHashUtil dbHash = new DbHashUtil(chunkManager, includeNamespaces);
		dbHash.call();
		return 0;
	}

	private void shutdown() {

	}

	private static void addShutdownHook(MongoSync sync) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.debug("**** SHUTDOWN *****");
				sync.shutdown();
			}
		}));
	}

	public static void main(String[] args) {
		MongoSync mongoSync = new MongoSync();
		addShutdownHook(mongoSync);

		int exitCode = 0;
		try {
			CommandLine cmd = new CommandLine(mongoSync);
			ParseResult parseResult = cmd.parseArgs(args);

			File defaultsFile;
			if (mongoSync.configFile != null) {
				defaultsFile = mongoSync.configFile;
			} else {
				defaultsFile = new File("mongosync.properties");
			}

			if (defaultsFile.exists()) {
				cmd.setDefaultValueProvider(new PropertiesDefaultProvider(defaultsFile));
			}
			parseResult = cmd.parseArgs(args);

			if (!CommandLine.printHelpIfRequested(parseResult)) {
				exitCode = mongoSync.call();
			}
		} catch (ParameterException ex) {
			System.err.println(ex.getMessage());
			ex.getCommandLine().usage(System.err);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.exit(exitCode);
	}

	@Override
	public void mongoSyncPaused(MongoSyncRunner runner) throws IOException {

		int pauseCount = mongosyncRunnersPausedCount.incrementAndGet();
		logger.debug("mongosync runner {} is paused, numPausedRunners: {}", runner.getId(), pauseCount);

		if (pauseCount == 1) {
			logger.debug("Coordinator mongosync has been paused, intializing collections and sharding");

			try {

				Set<String> existingDestNs = destShardClient.getCollectionsMap().keySet();
				logger.debug("dest cluster has {} collections total -- {}", existingDestNs.size(), existingDestNs);
				logger.debug("includeNamespaces: {}", includeNamespaces);

				shardConfigSync.syncMetadataOptimized();

			} catch (Exception e) {
				logger.warn("error in chunk init", e);
			}
			
			logger.debug("Starting mongosync followers");
			for (MongoSyncRunner mongosync : mongosyncRunners) {
				if (mongosync.equals(runner)) {
					continue;
				}
				try {
					mongosync.start();
				} catch (IOException e) {
					logger.warn("error starting mongosync {}", mongosync.getId());
					mongosync.start();
				}
			}
			
			//MongoSyncStatus status = runner.checkStatus();
			runner.resume();
			runner.waitForRunningState();

		} else {
			logger.warn("more than 1 mongosync appears to have been paused, current paused runner: {}", runner.getId());
		}
	}

}