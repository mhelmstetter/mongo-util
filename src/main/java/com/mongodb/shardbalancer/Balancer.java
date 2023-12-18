package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "balancer", mixinStandardHelpOptions = true, version = "balancer 0.1", description = "Custom mongodb shard balancer")
public class Balancer implements Callable<Integer> {

	@Option(names = { "--config" }, required = false, defaultValue = "balancer.properties")
	private String configFile;

	// @Option(names = { "-h", "--help", "-?", "-help"})
	// private boolean help;

	protected static final Logger logger = LoggerFactory.getLogger(Balancer.class);

	private final static String SOURCE_URI = "source";
	private final static String SOURCE_SHARDS = "sourceShards";
	private final static String INCLUDE_NAMESPACES = "includeNamespaces";
	private final static String CHECKPOINT_INTERVAL = "checkpointIntervalMinutes";
	private final static String ANALYZER_SLEEP_INTERVAL = "analyzerSleepIntervalMinutes";
	private final static String BALANCER_CHUNK_BATCH_SIZE = "balancerChunkBatchSize";

	private BalancerConfig balancerConfig;

	private ShardClient sourceShardClient;

	private TailingOplogAnalyzer oplogAnalyzer;

	private boolean stopped;

	private ChunkStats chunkStats;

	Map<String, NavigableMap<String, CountingMegachunk>> chunkMap;

	public void init() {
		Set<String> sourceShards = balancerConfig.getSourceShards();

		if (sourceShards.isEmpty()) {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri());
		} else {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri(), sourceShards, null);
		}

		balancerConfig.setSourceShardClient(sourceShardClient);
		sourceShardClient.init();
		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap();

		if (!sourceShardClient.isMongos()) {
			throw new IllegalArgumentException("Expected 'source' property to be a mongos");
		}

		balancerConfig.setStatsCollection(sourceShardClient.getCollection(balancerConfig.getStatsNamespace()));

		ChunkManager chunkManager = new ChunkManager(balancerConfig);
		chunkManager.initializeChunkQuery();
		BsonDocument chunkQuery = chunkManager.getChunkQuery();
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);

		//List<Megachunk> megaChunks = chunkManager.getMegaChunks(sourceChunksCache);
		//logger.debug("{} mega chunks", megaChunks.size());

		int uberThreshold = (sourceChunksCache.size() >= 1000) ? 300 : 100;

		chunkMap = new HashMap<>();

		int uberId = 0;
		int i = 0;
		for (RawBsonDocument m : sourceChunksCache.values()) {

			if (i++ % uberThreshold == 0) {
				uberId++;
			}

			CountingMegachunk mega = new CountingMegachunk();
			mega.setUberId(uberId);

			String ns = m.getString("ns").getValue();
			mega.setNs(ns);
			mega.setShard(m.getString("shard").getValue());
			
			NavigableMap<String, CountingMegachunk> innerMap = chunkMap.get(ns);
			if (innerMap == null) {
				innerMap = new TreeMap<>();
				chunkMap.put(ns, innerMap);
			}
			
			// Document collDoc = this.sourceShardClient.getCollectionsMap().get(ns);

			BsonDocument min = m.getDocument("min");
			mega.setMin(min);
			
			BsonValue max = m.get("max");
			if (max instanceof BsonMaxKey) {
				System.out.println();
			} else if (max instanceof BsonDocument) {
				mega.setMax((BsonDocument)max);
			} else {
				logger.error("unexpected max type: {}", max);
			}
			//;
			
			BsonValue val = min.get("_id");
			if (val == null) {
				logger.error("could not get _id shard key from chunk: {}", mega);
				continue;
			}
			if (val instanceof BsonMinKey) {
				innerMap.put("\u0000", mega);
			} else {
				innerMap.put(((BsonString) val).getValue(), mega);
			}
		}
		balancerConfig.setChunkMap(chunkMap);
	}

	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();
		oplogAnalyzer = new TailingOplogAnalyzer(balancerConfig);
		oplogAnalyzer.start();

		// Thread.sleep(300000);
		updateChunkStats();

		while (!stopped) {

			if (chunkStats.isEmpty()) {
				logger.debug("chunkStats empty, sleeping");
				Thread.sleep(30000);
				continue;
			}

			String ns = chunkStats.getHighestPriorityNamespace();
			String hottest = chunkStats.getHottestShard(ns);
			String coldest = chunkStats.getColdestShard(ns);
			logger.debug("{} - hottest shard: {}, coldest shard: {}", ns, hottest, coldest);
			
			List<String> shardPairs = chunkStats.getHottestColdestPairs(ns);

			// Print or process the shard pairs as needed
			for (int i = 0; i < shardPairs.size(); i += 2) {
			    String hottestShard = shardPairs.get(i);
			    String coldestShard = shardPairs.get(i + 1);
			    System.out.println("Pair " + (i / 2 + 1) + ": Hottest Shard = " + hottestShard + ", Coldest Shard = " + coldestShard);
			}

			List<Document> hotChunks = getHotChunks(hottest, ns);
			logger.debug("fetched the hottest {} chunks on {}", hotChunks.size(), hottest);
			

			int i = 1;
			for (Document chunkDoc : hotChunks) {
				String id = chunkDoc.getString("id");
				
				NavigableMap<String, CountingMegachunk> innerMap = chunkMap.get(ns);
				
				CountingMegachunk mega = innerMap.get(id);

				logger.debug("move chunk [ {} / {} ]: {}", i++, hotChunks.size(), mega);
				
				boolean success = sourceShardClient.moveChunk(ns, mega.getMin(), mega.getMax(), coldest, false, false, false, false);
				if (success) {
					mega.setShard(coldest);
				}
			}

			Thread.sleep(30000);

		}

		return 0;

	}


	private Date nowMinus24Hours() {
		Date currentDate = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentDate);
		calendar.add(Calendar.HOUR_OF_DAY, -24);
		Date twentyFourHoursAgo = calendar.getTime();
		return twentyFourHoursAgo;
	}

	private void updateChunkStats() {

		chunkStats = new ChunkStats();
		Date nowMinus24 = nowMinus24Hours();
		AggregateIterable<Document> results = balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(match(gte("startTime", nowMinus24)),
						group(fields(eq("ns", "$ns"), eq("shard", "$shard")), Accumulators.sum("totalOps", "$total"),
								Accumulators.sum("activeChunks", "$activeChunks")),
						sort(orderBy(ascending("_id.ns"), descending("totalOps")))));

		for (Document result : results) {
			Document id = result.get("_id", Document.class);
			ChunkStatsEntry chunkStatsEntry = new ChunkStatsEntry(id.getString("ns"), id.getString("shard"),
					result.getLong("totalOps"), result.getInteger("activeChunks"));
			chunkStats.addEntry(chunkStatsEntry);
		}
	}

	private List<Document> getHotChunks(String shard, String ns) {

		chunkStats = new ChunkStats();
		Date nowMinus24 = nowMinus24Hours();
		// AggregateIterable<Document> results =
		List<Document> results = new ArrayList<>();
		balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(match(and(gte("startTime", nowMinus24), eq("ns", ns), eq("shard", shard))),
						project(fields(excludeId())),
						unwind("$chunks"), project(fields(computed("id", "$chunks.id"), computed("count", "$chunks.cnt"))),
						sort(orderBy(descending("count"))), limit(balancerConfig.getBalancerChunkBatchSize())))
				.into(results);

		return results;
	}

	protected void parseArgs() throws ConfigurationException {

		Configuration config = readProperties();
		this.balancerConfig = new BalancerConfig();
		balancerConfig.setSourceClusterUri(config.getString(SOURCE_URI));
		String[] includes = config.getStringArray(INCLUDE_NAMESPACES);
		balancerConfig.setNamespaceFilters(includes);

		String[] sourceShards = config.getStringArray(SOURCE_SHARDS);
		balancerConfig.setSourceShards(sourceShards);
		balancerConfig.setAnalyzerSleepIntervalMinutes(config.getInt(ANALYZER_SLEEP_INTERVAL, 15));
		balancerConfig.setCheckpointIntervalMinutes(config.getInt(CHECKPOINT_INTERVAL, 15));
		balancerConfig.setBalancerChunkBatchSize(config.getInt(BALANCER_CHUNK_BATCH_SIZE, 1000));
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
		Balancer balancer = new Balancer();
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
		// shutdown = true;
	}

}
