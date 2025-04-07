package com.mongodb.dbhash;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.dbhash.model.DbHashResult;
import com.mongodb.dbhash.model.ShardDatabasePair;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.shardsync.SyncConfiguration;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "dbHashUtil", mixinStandardHelpOptions = true, version = "dbHashUtil 0.1", description = "MongoDB dbHash utility", defaultValueProvider = PropertiesDefaultProvider.class)
public class DbHashUtil implements Callable<Integer> {

	protected static final Logger logger = LoggerFactory.getLogger(DbHashUtil.class);

	@Option(names = { "--config", "-c" }, description = "config file", required = false)
	private File configFile;

	@Option(names = { "--source" }, description = "source mongodb uri connection string", required = false)
	private String sourceUri;

	@Option(names = { "--dest" }, description = "destination mongodb uri connection string", required = false)
	private String destUri;

	@Option(names = { "--includeNamespaces" }, description = "Namespaces to include", required = false)
	private Set<String> includeNamespaces;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;
	private ChunkManager chunkManager;
	
	public DbHashUtil(ChunkManager chunkManager, Set<String> includeNamespaces) {
		this.chunkManager = chunkManager;
		this.sourceShardClient = chunkManager.getConfig().getSourceShardClient();
		this.destShardClient = chunkManager.getConfig().getDestShardClient();
		this.includeNamespaces = includeNamespaces;
	}
	
	public DbHashUtil() {
	}

	private void initialize() throws IOException {
		
		SyncConfiguration shardConfigSyncConfig = new SyncConfiguration();
		shardConfigSyncConfig.setSourceClusterUri(sourceUri);
		shardConfigSyncConfig.setDestClusterUri(destUri);
		shardConfigSyncConfig.setNamespaceFilters(includeNamespaces.toArray(new String[0]));
		
		chunkManager = new ChunkManager(shardConfigSyncConfig);
		chunkManager.initalize();
		
		this.sourceShardClient = shardConfigSyncConfig.getSourceShardClient();
		this.destShardClient = shardConfigSyncConfig.getDestShardClient();
		
		sourceShardClient.populateShardMongoClients();
		destShardClient.populateShardMongoClients();
	}

	@Override
	public Integer call() throws Exception {
		//initialize();

		// Map<String, Shard> map = sourceShardClient.getShardsMap();

		List<Namespace> includes = null;
		Set<String> dbNames = new HashSet<>();
		if (includeNamespaces != null && !includeNamespaces.isEmpty()) {
			includes = new ArrayList<>();
			for (String nsStr : includeNamespaces) {
				Namespace ns = new Namespace(nsStr);
				dbNames.add(ns.getDatabaseName());

				includes.add(ns);
			}
		}

		List<ShardDatabasePair> pairs = new ArrayList<>();

		Iterator<String> destShards = destShardClient.getShardsMap().keySet().iterator();

		for (String sourceShard : sourceShardClient.getShardsMap().keySet()) {
			String destShard = destShards.next();

			for (Namespace ns : includes) {
				ShardDatabasePair pair = new ShardDatabasePair(sourceShard, destShard, ns);
				pairs.add(pair);
			}
		}
		compareDbHashes(pairs);

		return 0;
	}
	
	private void populateResultShardMapping(DbHashResult result) {
		
		String sourceShard = result.getSourceShard();
		String destShard = result.getDestShard();
		if (sourceShard == null) {
			sourceShard = chunkManager.getDestToSourceShardMapping(destShard);
			result.setSourceShard(sourceShard);
		} else {
			destShard = chunkManager.getSourceToDestShardMap().get(sourceShard);
			result.setDestShard(destShard);
		}
		
	}

	public void compareDbHashes(List<ShardDatabasePair> shardDatabasePairs) throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(sourceShardClient.getShardsMap().size());
		CompletionService<DbHashResult> completionService = new ExecutorCompletionService<>(executor);

		// Submit both source and destination dbHash tasks for all shards and 1 database
		for (ShardDatabasePair pair : shardDatabasePairs) {
			// Submit source dbHash task
			MongoClient sourceClient = sourceShardClient.getShardMongoClient(pair.getSourceShard());
			completionService.submit(new DbHashTask(sourceClient, pair, true));

			// Submit destination dbHash task
			MongoClient destClient = destShardClient.getShardMongoClient(pair.getDestinationShard());
			completionService.submit(new DbHashTask(destClient, pair, false));
		}

		// A set to keep track of unmatched dbHash results
		// Set<DbHashResult> resultSet = new HashSet<>();

		Map<Namespace, Map<String, MutablePair<String, String>>> namespaceShardMap = new HashMap<>();

		// Retrieve and process results as they complete
		for (int i = 0; i < shardDatabasePairs.size() * 2; i++) {
			Future<DbHashResult> future = completionService.take(); // Blocks until a result is ready
			DbHashResult result = future.get();
			populateResultShardMapping(result);
			Namespace ns = result.getNamespace();

			Map<String, MutablePair<String, String>> innerMap = namespaceShardMap.get(ns);
			MutablePair<String, String> pair = null;

			if (innerMap == null) {
				innerMap = new HashMap<>();
				namespaceShardMap.put(ns, innerMap);
				pair = new MutablePair<>();
				innerMap.put(result.getShardPairKey(), pair);
			} else {
				pair = innerMap.get(result.getShardPairKey());
				if (pair == null) {
					pair = new MutablePair<>();
					innerMap.put(result.getShardPairKey(), pair);
				}
			}

			if (result.isSource()) {
				pair.setLeft(result.getHash());
			} else {
				pair.setRight(result.getHash());
			}
		}
		
		
		Map<String, Shard> shardMap = destShardClient.getShardsMap();
		for (String key : shardMap.keySet()) {
			Shard s = shardMap.get(key);
			logger.debug("{} ==> {}", key, s);
		}

		for (Map.Entry<Namespace, Map<String, MutablePair<String, String>>> entry : namespaceShardMap.entrySet()) {

			Namespace ns = entry.getKey();
			logger.debug("***** {} *****", ns);
			Map<String, MutablePair<String, String>> innerMap = entry.getValue();

			for (Map.Entry<String, MutablePair<String, String>> innerEntry : innerMap.entrySet()) {
				MutablePair<String, String> pair = innerEntry.getValue();
				if (pair.left.equals(pair.right)) {
					logger.debug("    shard: {}, sourceHash: {}, destHash: {} -- ✅ PASS", innerEntry.getKey(), pair.left, pair.right);
				} else {
					logger.debug("    shard: {}, sourceHash: {}, destHash: {} -- ❌ FAIL", innerEntry.getKey(), pair.left, pair.right);
				}
				
				
			}

		}
		
		executor.shutdown();

	}

	private void shutdown() {

	}

	private static void addShutdownHook(DbHashUtil sync) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.debug("**** SHUTDOWN *****");
				sync.shutdown();
			}
		}));
	}

	public static void main(String[] args) {
		DbHashUtil dbHash = new DbHashUtil();
		addShutdownHook(dbHash);

		int exitCode = 0;
		try {
			CommandLine cmd = new CommandLine(dbHash);
			ParseResult parseResult = cmd.parseArgs(args);
			File defaultsFile = null;
			if (dbHash.configFile != null) {
				defaultsFile = dbHash.configFile;
			}

			if (defaultsFile != null && defaultsFile.exists()) {
				cmd.setDefaultValueProvider(new PropertiesDefaultProvider(defaultsFile));
			}

			parseResult = cmd.parseArgs(args);

			if (!CommandLine.printHelpIfRequested(parseResult)) {
				dbHash.initialize();
				exitCode = dbHash.call();
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

}