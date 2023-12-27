package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.computed;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.BsonBinary;
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
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonUuidUtil;
import com.mongodb.util.bson.BsonValueWrapper;

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
	private final static String DRY_RUN = "dryRun";
	private final static String DELTA_THRESHOLD_RATIO = "deltaThresholdRatio";

	private BalancerConfig balancerConfig;

	private ShardClient sourceShardClient;

	private TailingOplogAnalyzer oplogAnalyzer;

	//private Timer timer;

	private AtomicBoolean stopped = new AtomicBoolean(false);

	private ChunkStats chunkStats;

	Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap;

	public void init() {
		Set<String> sourceShards = balancerConfig.getSourceShards();

		if (sourceShards.isEmpty()) {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri());
		} else {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri(), sourceShards, null);
		}
		
		balancerConfig.setSourceShardClient(sourceShardClient);
		sourceShardClient.init();
		
		sourceShardClient.stopBalancer();
		sourceShardClient.enableAutosplit();
		
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
		
		balancerConfig.getStatsCollection().createIndex(new Document("analysisId", 1));
		balancerConfig.getStatsCollection().createIndex(new Document("endTime", 1), new IndexOptions().expireAfter(43200L, TimeUnit.SECONDS));

		ChunkManager chunkManager = new ChunkManager(balancerConfig);
		chunkManager.setSourceShardClient(sourceShardClient);
		chunkManager.initializeChunkQuery();
		BsonDocument chunkQuery = chunkManager.getChunkQuery();
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);

		// List<Megachunk> megaChunks = chunkManager.getMegaChunks(sourceChunksCache);
		// logger.debug("{} mega chunks", megaChunks.size());

		int uberThreshold = (sourceChunksCache.size() >= 1000) ? 300 : 100;

		chunkMap = new HashMap<>();

		int uberId = 0;
		int i = 0;
		for (RawBsonDocument chunkDoc : sourceChunksCache.values()) {

			if (i++ % uberThreshold == 0) {
				uberId++;
			}

			CountingMegachunk mega = new CountingMegachunk();
			mega.setUberId(uberId);
			
			String ns = null;
			if (chunkDoc.containsKey("ns")) {
				ns = chunkDoc.getString("ns").getValue();
			} else {
				BsonBinary buuid = chunkDoc.getBinary("uuid");
				UUID uuid = BsonUuidUtil.convertBsonBinaryToUuid(buuid);
				ns = sourceShardClient.getCollectionsUuidMap().get(uuid);
			}
			
			Document collMeta = this.sourceShardClient.getCollectionsMap().get(ns);
			Document shardKeysDoc = (Document) collMeta.get("key");
			Set<String> shardKeys = shardKeysDoc.keySet();
			
			mega.setNs(ns);
			mega.setShard(chunkDoc.getString("shard").getValue());

			NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = chunkMap.get(ns);
			if (innerMap == null) {
				innerMap = new TreeMap<>();
				chunkMap.put(ns, innerMap);
			}

			// Document collDoc = this.sourceShardClient.getCollectionsMap().get(ns);

			BsonDocument min = chunkDoc.getDocument("min");
			mega.setMin(min);

			BsonValue max = chunkDoc.get("max");
			if (max instanceof BsonMaxKey) {
				logger.warn("*** BsonMaxKey not handled");
			} else if (max instanceof BsonDocument) {
				mega.setMax((BsonDocument) max);
			} else {
				logger.error("unexpected max type: {}", max);
			}

			BsonValue val = null;
			if (shardKeys.size() == 1) {
				val = min.get(min.getFirstKey());
			} else {
				logger.warn("compound not implemented");
			}
			 
			if (val == null) {
				logger.error("could not get shard key from chunk: {}", mega);
				continue;
			}
			
			if (val instanceof BsonMinKey) {
				//innerMap.put("\u0000", mega);
				innerMap.put(new BsonValueWrapper(val), mega);
			} else if (val instanceof BsonString) {
				//innerMap.put(((BsonString) val).getValue(), mega);
				innerMap.put(new BsonValueWrapper(val), mega);
			} else {
				//logger.debug("chunkDoc min._id was unexepected type: {}, chunk: {}", val.getClass().getName(), mega);
				innerMap.put(new BsonValueWrapper(val), mega);
			}
		}
		balancerConfig.setChunkMap(chunkMap);
		
	}



	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();

		oplogAnalyzer = new TailingOplogAnalyzer(balancerConfig);

		int iteration = 1;
		
		while (!stopped.get()) {
			
			logger.debug("Balancer call() iteration {}", iteration++);
			
			oplogAnalyzer.start();
			Thread.sleep(balancerConfig.getAnalyzerSleepIntervalMillis());
			oplogAnalyzer.stop();
			updateChunkStats();
			
			List<Document> nsStats = this.getNamespaceStats();
			for (Document d : nsStats) {
				
				String ns = d.getString("ns");
				
				chunkStats.updateTargetOpsPerShard(ns, balancerConfig.getDeltaThresholdRatio());
				List<ChunkStatsEntry> entries = chunkStats.getEntries(ns);
				if (entries == null || entries.isEmpty()) {
					logger.debug("no ChunkStatsEntry for ns: {}, exiting loop for this ns", ns);
					continue;
				}
				
				int negativeChunksToMoveCount = 0;
				for (ChunkStatsEntry e : entries) {
					if (e.getChunksToMove() < 0 && e.isAboveThreshold()) {
						negativeChunksToMoveCount++;
					}
				}
				
				logger.debug("About to balance {}, for {} iterations / shard pairs", ns, negativeChunksToMoveCount);
				
				if (balancerConfig.isDryRun()) {
					continue;
				}
				
				for (int entryNum = 0; entryNum < negativeChunksToMoveCount; entryNum++) {
					
					ChunkStatsEntry from = entries.get(entryNum);
					ChunkStatsEntry to = entries.get(entries.size() - 1 - entryNum);
					
					int numChunks = (int)(from.getChunksToMove() * -0.1);
					
					if (numChunks <= 0) {
						logger.debug("{}: no chunks to move this round, below threshold", ns);
						continue;
					}
					logger.debug("{}: will move {} chunks from {} to {}", ns, numChunks, from.getShard(), to.getShard());
					
					List<Document> hotChunks = getHotChunks(ns, from.getShard(), to.getShard(), numChunks);
					
					int i = 1;
					for (Document chunkDoc : hotChunks) {
	
						String id = null;
						Object v = chunkDoc.get("id");
						if (v instanceof String) {
							id = (String) v;
						} else {
							System.out.println();
						}
	
						NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = chunkMap.get(ns);
	
						CountingMegachunk mega = innerMap.get(id);
	
						logger.debug("move chunk [ {} / {} ]: {}, _id: {}", i++, hotChunks.size(), mega, chunkDoc.get("_id"));
	
						boolean success = sourceShardClient.moveChunk(ns, mega.getMin(), mega.getMax(), to.getShard(), false, false,
								false, false);
						
						if (success) {
							mega.setShard(to.getShard());
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
					
				}
			}
			
			Thread.sleep(30000);

		}

		return 0;

	}

	private void updateChunkStats() {

		chunkStats = new ChunkStats();
		AggregateIterable<Document> results = balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(
						match(eq("analysisId", balancerConfig.getAnalysisId())),
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
		logger.debug("updateChunkStats(), got {} results from chunkStats collection, ns count: {}", resultNum, chunkStats.size());
	}
	
	private List<Document> getNamespaceStats() {

		List<Document> results = new ArrayList<>();
		
		balancerConfig.getStatsCollection()
			.aggregate(Arrays.asList(
				match(eq("analysisId", balancerConfig.getAnalysisId())),
				group(fields(eq("ns", "$ns"), eq("shard", "$shard")), Accumulators.sum("totalOps", "$total"),
						Accumulators.sum("activeChunks", "$activeChunks")),
				
				project(fields(computed("ns", "$_id.ns"), computed("shard", "$_id.shard"), include("totalOps"),
						include("activeChunks"))),
				
				group(fields(eq("ns", "$ns")), Accumulators.sum("totalOps", "$totalOps"),
						//Accumulators.sum("activeChunks", "$activeChunks"), 
						Accumulators.min("minTotalOps", "$totalOps"), Accumulators.max("maxTotalOps", "$totalOps")),
				
				Aggregates.project(Projections.fields(
                        Projections.excludeId(),
                        Projections.computed("ns", "$_id.ns"),
                        Projections.include("totalOps"),
                        Projections.computed("deltaOps", Document.parse("{$subtract: ['$maxTotalOps', '$minTotalOps']}"))
                )),
				
				sort(orderBy(descending("deltaOps")))
		
		)).into(results);

		return results;
	}
	

	private List<Document> getHotChunks(String ns, String fromShard, String toShard, int limit) {

		chunkStats = new ChunkStats();
		List<Document> results = new ArrayList<>();
		balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(
						match(and(eq("analysisId", balancerConfig.getAnalysisId()), eq("ns", ns), eq("shard", fromShard))),
						// project(fields(excludeId())),
						unwind("$chunks"),
						project(fields(computed("id", "$chunks.id"), computed("count", "$chunks.cnt"),
								computed("balanced", "$chunks.balanced"))),
						match(in("balanced", null, false)), sort(orderBy(descending("count"))),
						limit(limit)))
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
		
		balancerConfig.setDryRun(config.getBoolean(DRY_RUN, false));
		
		balancerConfig.setDeltaThresholdRatio(config.getDouble(DELTA_THRESHOLD_RATIO, 0.045));
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
