package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.RandomUtils;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "balancer", mixinStandardHelpOptions = true, version = "balancer 0.1", description = "Custom mongodb shard balancer")
public class ShardRemovalBalancer implements Callable<Integer> {

	@Option(names = { "--config" }, required = false, defaultValue = "balancer.properties")
	private String configFile;

	protected final Logger logger = LoggerFactory.getLogger(ShardRemovalBalancer.class);

	private final static String SOURCE_URI = "source";
	private final static String SOURCE_SHARDS = "sourceShards";
	private final static String DEST_SHARDS = "destShards";
	private final static String INCLUDE_NAMESPACES = "includeNamespaces";
	private final static String DRY_RUN = "dryRun";
	private final static String LIMIT = "limit";

	private BalancerConfig balancerConfig;

	private ShardClient sourceShardClient;

	private ChunkManager chunkManager;

	private int limit;

	Map<String, RawBsonDocument> sourceChunksCache;

	public void init() {
		Set<String> sourceShards = balancerConfig.getSourceShards();

		sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri(), sourceShards, null);

		balancerConfig.setSourceShardClient(sourceShardClient);
		sourceShardClient.init();

		sourceShardClient.stopBalancer();

		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap();

		if (!sourceShardClient.isMongos()) {
			throw new IllegalArgumentException("Expected 'source' property to be a mongos");
		}

		chunkManager = new ChunkManager(balancerConfig);
		chunkManager.setSourceShardClient(sourceShardClient);
		chunkManager.initializeSourceChunkQuery();
		sourceChunksCache = new LinkedHashMap<>();
		loadChunkMap(null);
	}

	private void loadChunkMap(String namespace) {

		logger.debug("Starting loadChunkMap, size: {}");
		BsonDocument chunkQuery = null;

		if (namespace == null) {
			chunkQuery = chunkManager.getSourceChunkQuery();
		} else {
			chunkQuery = chunkManager.newChunkQuery(sourceShardClient, namespace);
		}
		sourceShardClient.loadChunksCache(chunkQuery, sourceChunksCache);
	}

	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();

		MongoCollection<RawBsonDocument> chunksColl = sourceShardClient.getChunksCollectionRaw();

		int moveCount = 0;

		for (RawBsonDocument chunk : sourceChunksCache.values()) {

			String currentShard = chunk.getString("shard").getValue();
			int rangesMoved = 0;

			while (moveCount < limit) {

				String destShard = RandomUtils.getRandomElementFromSet(balancerConfig.getDestShards());

				if (currentShard.equals(destShard)) {
					logger.debug("currentShard == destShard, all chunks moved");
					break;
				}

				String ns = chunk.getString("ns").getValue();
				BsonDocument min = (BsonDocument) chunk.get("min");
				BsonObjectId id = chunk.getObjectId("_id");
				//BsonDocument max = (BsonDocument) chunk.get("max");
				sourceShardClient.moveRange(ns, min, destShard, balancerConfig.isDryRun());
 				
				rangesMoved++;
				moveCount++;
				logger.debug("{}: moved range with min: {} to shard {}, rangesMoved: {}, totalMoved: {} - _id: {}", ns, min,
						destShard, rangesMoved, moveCount, id);

				Bson filter = and(eq("uuid", chunk.get("uuid")), eq("min", min));
				RawBsonDocument ch = chunksColl.find(filter).first();
				currentShard = ch.getString("shard").getValue();
			}

		}

		return 0;

	}

	protected void parseArgs() throws ConfigurationException {

		Configuration config = readProperties();
		this.balancerConfig = new BalancerConfig();
		balancerConfig.setSourceClusterUri(config.getString(SOURCE_URI));
		String[] includes = config.getStringArray(INCLUDE_NAMESPACES);
		balancerConfig.setNamespaceFilters(includes);

		String[] sourceShards = config.getStringArray(SOURCE_SHARDS);
		balancerConfig.setSourceShards(sourceShards);

		String[] destShards = config.getStringArray(DEST_SHARDS);
		balancerConfig.setDestShards(destShards);

		balancerConfig.setDryRun(config.getBoolean(DRY_RUN, false));

		this.limit = config.getInt(LIMIT, Integer.MAX_VALUE);
	}

	private Configuration readProperties() throws ConfigurationException {

		FileBasedConfigurationBuilder<PropertiesConfiguration> builder = new FileBasedConfigurationBuilder<>(
				PropertiesConfiguration.class)
				.configure(new Parameters().properties().setFileName(configFile).setThrowExceptionOnMissing(true)
						.setListDelimiterHandler(new DefaultListDelimiterHandler(',')).setIncludesAllowed(false));
		PropertiesConfiguration config = null;
		config = builder.getConfiguration();
		return config;
	}

	public static void main(String[] args) {
		ShardRemovalBalancer balancer = new ShardRemovalBalancer();
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				System.out.println();
				System.out.println("**** SHUTDOWN *****");
				balancer.stop();
			}
		}));

		int exitCode = new CommandLine(balancer).execute(args);
		System.exit(exitCode);
	}

	protected void stop() {
		logger.debug("**** SHUTDOWN *****");
	}

}
