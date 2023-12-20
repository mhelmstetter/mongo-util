package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.mql.MqlValues.*;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;
import com.mongodb.client.model.Updates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.bson.types.ObjectId;
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

	private Timer timer;

	private AtomicBoolean stopped = new AtomicBoolean(false);

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
		balancerConfig.setBalancerRoundCollection(
				sourceShardClient.getCollection(balancerConfig.getBalancerRoundNamespace()));
		balancerConfig.setBalancerStateCollection(
				sourceShardClient.getCollection(balancerConfig.getBalancerStateNamespace()));

		ChunkManager chunkManager = new ChunkManager(balancerConfig);
		chunkManager.initializeChunkQuery();
		BsonDocument chunkQuery = chunkManager.getChunkQuery();
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);

		// List<Megachunk> megaChunks = chunkManager.getMegaChunks(sourceChunksCache);
		// logger.debug("{} mega chunks", megaChunks.size());

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
				logger.warn("*** BsonMaxKey not handled");
			} else if (max instanceof BsonDocument) {
				mega.setMax((BsonDocument) max);
			} else {
				logger.error("unexpected max type: {}", max);
			}

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

		timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				updateBalancerState();
			}
		}, 0, balancerConfig.getBalancerPollIntervalMillis());
	}

	private void waitForChunkStats() throws InterruptedException {
		updateChunkStats();
		while (chunkStats.isEmpty()) {
			logger.debug("chunkStats empty, sleeping");
			Thread.sleep(30000);
			updateChunkStats();
		}
	}

	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();

		oplogAnalyzer = new TailingOplogAnalyzer(balancerConfig);
		oplogAnalyzer.start();

		//Thread.sleep(balancerConfig.getAnalyzerSleepIntervalMillis());

		waitForChunkStats();

		int iteration = 1;
		
		while (!stopped.get()) {
			
			//String ns = chunkStats.getHighestPriorityNamespace();
			
			logger.debug("Balancer call() iteration {}", iteration++);
			
			List<Document> nsStats = this.getNamespaceStats();
			for (Document d : nsStats) {
				logger.debug(d.toString());
			}
			
			String ns = nsStats.get(0).getString("ns");
			
			String hottest = chunkStats.getHottestShard(ns);
			String coldest = chunkStats.getColdestShard(ns);

			chunkStats.getTargetOpsPerShard(ns);

			logger.debug("{} - hottest shard: {}, coldest shard: {}", ns, hottest, coldest);

			List<Document> hotChunks = getHotChunks(hottest, ns);
			logger.debug("fetched the hottest {} chunks on {}", hotChunks.size(), hottest);

			int i = 1;
			for (Document chunkDoc : hotChunks) {

				String id = null;
				Object v = chunkDoc.get("id");
				if (v instanceof String) {
					id = (String) v;
				} else {
					System.out.println();
				}

				NavigableMap<String, CountingMegachunk> innerMap = chunkMap.get(ns);

				CountingMegachunk mega = innerMap.get(id);

				logger.debug("move chunk [ {} / {} ]: {}, _id: {}", i++, hotChunks.size(), mega, chunkDoc.get("_id"));

				boolean success = sourceShardClient.moveChunk(ns, mega.getMin(), mega.getMax(), coldest, false, false,
						false, false);
				if (success) {
					mega.setShard(coldest);
					balancerConfig.getStatsCollection().updateOne(
							and(eq("_id", chunkDoc.get("_id")), eq("chunks.id", id)),
							Updates.combine(
								Updates.set("chunks.$.balanced", true),
								Updates.inc("balancedChunks", 1)
							));
				}

				if (stopped.get()) {
					logger.debug("Balancer stop requested, terminating current balancing batch");
					break;
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

	private void updateBalancerState() {
		// BalancerState bs = new BalancerState();
		Document bsDoc = balancerConfig.getBalancerStateCollection().find().first();
		if (bsDoc == null) {
			bsDoc = new Document("runAnalyzer", false);
			balancerConfig.getBalancerStateCollection().insertOne(bsDoc);
		}
		Boolean runAnalyzer = bsDoc.getBoolean("runAnalyzer");
		logger.debug("updateBalancerState: runAnalyzer: {}", runAnalyzer);
		if (runAnalyzer) {
			ObjectId aid = new ObjectId();
			balancerConfig.setAnalysisId(aid);
			balancerConfig.getBalancerStateCollection().updateOne(empty(), Updates.set("analysisId", aid));
		}
		balancerConfig.setRunAnalyzer(runAnalyzer);
	}

	private void updateChunkStats() {

		chunkStats = new ChunkStats();
		Date nowMinus24 = nowMinus24Hours();
		AggregateIterable<Document> results = balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(match(gte("startTime", nowMinus24)),
						group(fields(eq("ns", "$ns"), eq("shard", "$shard")), Accumulators.sum("totalOps", "$total"),
								Accumulators.sum("activeChunks", "$activeChunks")),
						sort(orderBy(ascending("_id.ns"), descending("totalOps"))),
						project(fields(computed("ns", "$_id.ns"), computed("shard", "$_id.shard"), include("totalOps"),
								include("activeChunks")))));

		int resultNum = 0;
		for (Document result : results) {
			String ns = result.getString("ns");
			if (ns == null) {
				logger.warn("updateChunkStats(), ns from aggregate was null!");
			}
			ChunkStatsEntry chunkStatsEntry = new ChunkStatsEntry(ns, result.getString("shard"),
					result.getLong("totalOps"), result.getInteger("activeChunks"));
			chunkStats.addEntry(chunkStatsEntry);
			resultNum++;
		}
		logger.debug("updateChunkStats(), got {} results from chunkStats collection", resultNum);
	}
	
	private List<Document> getNamespaceStats() {

		List<Document> results = new ArrayList<>();
		
		balancerConfig.getStatsCollection()
			.aggregate(Arrays.asList(
				//match(gte("startTime", nowMinus24)),
				group(fields(eq("ns", "$ns"), eq("shard", "$shard")), Accumulators.sum("totalOps", "$total"),
						Accumulators.sum("activeChunks", "$activeChunks")),
				
				project(fields(computed("ns", "$_id.ns"), computed("shard", "$_id.shard"), include("totalOps"),
						include("activeChunks"))),
				
				group(fields(eq("ns", "$ns")), Accumulators.sum("totalOps", "$totalOps"),
						Accumulators.sum("activeChunks", "$activeChunks"), 
						Accumulators.min("minTotalOps", "$totalOps"), Accumulators.max("maxTotalOps", "$totalOps")),
				
				project(fields(computed("ns", "$_id"), include("totalOps"), include("activeChunks"),
						computed("deltaOps", current().getNumber("$maxTotalOps").subtract(current().getNumber("$minTotalOps")))
				)),
				
				sort(orderBy(descending("deltaOps")))
		)).into(results);

		return results;
	}
	

	private List<Document> getHotChunks(String shard, String ns) {

		chunkStats = new ChunkStats();
		Date nowMinus24 = nowMinus24Hours();
		// AggregateIterable<Document> results =
		List<Document> results = new ArrayList<>();
		balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(match(and(gte("startTime", nowMinus24), eq("ns", ns), eq("shard", shard))),
						// project(fields(excludeId())),
						unwind("$chunks"),
						project(fields(computed("id", "$chunks.id"), computed("count", "$chunks.cnt"),
								computed("balanced", "$chunks.balanced"))),
						match(in("balanced", null, false)), sort(orderBy(descending("count"))),
						limit(balancerConfig.getBalancerChunkBatchSize())))
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
