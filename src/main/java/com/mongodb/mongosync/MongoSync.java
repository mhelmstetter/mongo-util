package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.BaseConfiguration;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "mongosync", mixinStandardHelpOptions = true, version = "mongosync 0.1", description = "mongosync runner", defaultValueProvider = PropertiesDefaultProvider.class)
public class MongoSync implements Callable<Integer> {

	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);

	@Option(names = { "--config", "-c" }, description = "config file", required = false, defaultValue = "mongosync.properties")
	private File configFile;
	
	@Option(names = { "--logDir" }, description = "log path", required = false)
	private File logDir;

	@Option(names = { "--source" }, description = "source mongodb uri connection string", required = false)
	private String sourceUri;

	@Option(names = { "--dest" }, description = "destination mongodb uri connection string", required = false)
	private String destUri;

	@Option(names = { "--mongosyncBinary" }, description = "path to mongosync binary", required = false)
	private File mongosyncBinary;
	
	@Option(names = { "--buildIndexes" }, description = "Build indexes on target", required = false)
	private boolean buildIndexes = true;
	
	@Option(names = { "--includeNamespaces" }, description = "Namespaces to include", required = false)
	private List<String> includeNamespaces;

	private ChunkManager chunkManager;
	private ShardClient sourceShardClient;
	private ShardClient destShardClient;

	List<MongoSyncRunner> mongosyncRunners;

	private void initialize() throws IOException {

		BaseConfiguration config = new BaseConfiguration();
		config.setSourceClusterUri(sourceUri);
		config.setDestClusterUri(destUri);

		chunkManager = new ChunkManager(config);
		chunkManager.initalize();
		this.sourceShardClient = config.getSourceShardClient();
		this.destShardClient = config.getDestShardClient();

		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap();
		destShardClient.populateShardMongoClients();

		mongosyncRunners = new ArrayList<>(sourceShardClient.getShardsMap().size());
	}

	@Override
	public Integer call() throws Exception {
		initialize();
		
		List<Namespace> includes = null;
		if (! includeNamespaces.isEmpty()) {
			includes = new ArrayList<>();
			for (String ns : includeNamespaces) {
				includes.add(new Namespace(ns));
			}
		}

		int port = 27000;
		for (Shard source : sourceShardClient.getShardsMap().values()) {

			MongoSyncRunner mongosync = new MongoSyncRunner(source.getId());
			mongosyncRunners.add(mongosync);
			mongosync.setSourceUri(sourceUri);
			mongosync.setDestinationUri(destUri);
			mongosync.setMongosyncBinary(mongosyncBinary);
			mongosync.setPort(port++);
			mongosync.setLoadLevel(3);
			mongosync.setLogDir(logDir);
			mongosync.setIncludeNamespaces(includes);

			String destShardId = chunkManager.getShardMapping(source.getId());
			Shard dest = destShardClient.getShardsMap().get(destShardId);
			logger.debug(String.format("Creating MongoMirrorRunner for %s ==> %s", source.getId(), dest.getId()));
			mongosync.initialize();
		}
		
		for (MongoSyncRunner mongosync : mongosyncRunners) {
			mongosync.start();
		}
		

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

}