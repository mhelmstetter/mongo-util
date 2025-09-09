package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Aggregates.group;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
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

import com.mongodb.MongoCommandException;
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

import ch.qos.logback.classic.ClassicConstants;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "balancer", mixinStandardHelpOptions = true, version = "balancer 0.1", description = "Custom mongodb shard balancer")
public class Balancer implements Callable<Integer> {

	@Option(names = { "--config" }, required = false, defaultValue = "balancer.properties")
	private String configFile;

	// @Option(names = { "-h", "--help", "-?", "-help"})
	// private boolean help;

	protected final Logger logger;

	private final static String SOURCE_URI = "source";
	private final static String SOURCE_SHARDS = "sourceShards";
	private final static String INCLUDE_NAMESPACES = "includeNamespaces";
	private final static String ANALYZER_SLEEP_INTERVAL = "analyzerSleepIntervalMinutes";
	private final static String DRY_RUN = "dryRun";
	private final static String DELTA_THRESHOLD_PERCENT = "deltaThresholdPercent";
	private final static String MOVE_COUNT_BACKOFF_THRESHOLD = "moveCountBackoffThreshold";
	private final static String ACTIVE_CHUNK_THRESHOLD = "activeChunkThreshold";
	private final static String MAX_DOCS = "maxDocs";
	private final static String DEST_SHARDS = "destShards";

	private BalancerConfig balancerConfig;

	private ShardClient sourceShardClient;
	
	private ChunkManager chunkManager;

	private TailingOplogAnalyzer oplogAnalyzer;

	//private Timer timer;

	private AtomicBoolean stopped = new AtomicBoolean(false);

	private ChunkStats chunkStats;

	Map<String, RawBsonDocument> sourceChunksCache;
	Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap;
	
	private int backoffSleepMinutes = 0;
	
	public Balancer() {
		// setup logger here so system property can be set first
		logger = LoggerFactory.getLogger(Balancer.class);
	}

	public void init() {
		Set<String> sourceShards = balancerConfig.getSourceShards();

		if (sourceShards.isEmpty()) {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri());
		} else {
			sourceShardClient = new ShardClient("source", balancerConfig.getSourceClusterUri(), sourceShards);
		}
		
		balancerConfig.setSourceShardClient(sourceShardClient);
		sourceShardClient.init();
		
		sourceShardClient.stopBalancer();
		//sourceShardClient.enableAutosplit();
		
		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap();

		if (!sourceShardClient.isMongos()) {
			throw new IllegalArgumentException("Expected 'source' property to be a mongos");
		}

		balancerConfig.setStatsCollection(sourceShardClient.getCollectionBson(balancerConfig.getStatsNamespace()));
		balancerConfig.setBalancerRoundCollection(
				sourceShardClient.getCollection(balancerConfig.getBalancerRoundNamespace()));
		balancerConfig.setBalancerStateCollection(
				sourceShardClient.getCollection(balancerConfig.getBalancerStateNamespace()));
		
		balancerConfig.getStatsCollection().createIndex(new Document("analysisId", 1));
		balancerConfig.getStatsCollection().createIndex(new Document("endTime", 1), new IndexOptions().expireAfter(43200L, TimeUnit.SECONDS));

		chunkManager = new ChunkManager(balancerConfig);
		chunkManager.setSourceShardClient(sourceShardClient);
		chunkManager.initializeSourceChunkQuery();
		chunkMap = new HashMap<>();
		sourceChunksCache = new LinkedHashMap<>();
		chunkManager.loadChunkMap(null, sourceChunksCache, chunkMap);
		balancerConfig.setChunkMap(chunkMap);
	}
	
	private void backoffSleep() {
		if (backoffSleepMinutes == 0) {
			backoffSleepMinutes = 5;
		} else {
			if (backoffSleepMinutes > 180) {
				backoffSleepMinutes = 5;
			} else {
				backoffSleepMinutes = backoffSleepMinutes * 2;
			}
		}
		logger.debug("chunksMoved last round was <= threshold, backoff sleep for {} minutes", backoffSleepMinutes);
		try {
			Thread.sleep(backoffSleepMinutes * 1000 * 60);
		} catch (InterruptedException e) {
		}
	}

	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();

		oplogAnalyzer = new TailingOplogAnalyzer(balancerConfig);

		int iteration = 1;
		int moveCount = 0;
		
		
		while (!stopped.get()) {
			
			logger.debug("Balancer call() iteration {}, last round move count: {}", iteration++, moveCount);
			
			
			if (moveCount <= balancerConfig.getMoveCountBackoffThreshold() && iteration > 2) {
				backoffSleep();
			} else {
				backoffSleepMinutes = 5;
			}
				
			moveCount = 0;
			
			oplogAnalyzer.start();
			Thread.sleep(balancerConfig.getAnalyzerSleepIntervalMillis());
			oplogAnalyzer.stop();
			updateChunkStats();
			
			Set<String> shardsSet = this.sourceShardClient.getShardsMap().keySet();
			
			List<BsonDocument> nsStats = this.getNamespaceStats();
			for (BsonDocument d : nsStats) {
				
				String ns = d.getString("ns").getValue();
				
				chunkStats.updateTargetOpsPerShard(ns, balancerConfig.getDeltaThresholdPercent(), shardsSet, balancerConfig.getActiveChunkThreshold());
				List<ChunkStatsEntry> entries = chunkStats.getEntries(ns);
				if (entries == null || entries.isEmpty()) {
					logger.debug("no ChunkStatsEntry for ns: {}, exiting loop for this ns", ns);
					continue;
				}
				
				int negativeChunksToMoveCount = 0;
				for (ChunkStatsEntry e : entries) {
					if (e.isAboveThreshold()) {
						negativeChunksToMoveCount++;
					}
					
					// max out to 2 iterations
					if (negativeChunksToMoveCount >= 2) {
						break;
					}
				}
				
				logger.debug("About to balance {}, for {} iterations / shard pairs", ns, negativeChunksToMoveCount);
				
				if (balancerConfig.isDryRun()) {
					continue;
				}
				
				NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = chunkMap.get(ns);
				
				for (int entryNum = 0; entryNum < negativeChunksToMoveCount; entryNum++) {
					
					ChunkStatsEntry from = entries.get(entryNum);
					ChunkStatsEntry to = entries.get(entries.size() - 1 - entryNum);
					
					if (!from.isAboveThreshold() || (!to.isAboveThreshold() && to.getTotalOps() > 0)) {
						logger.debug("source and/or target shard is not above threshold, skipping. source: {}, target: {}", from.getShard(), to.getShard());
						continue;
					}
					
					// Check if destShards is configured and if target shard is allowed
					Set<String> destShards = balancerConfig.getDestShards();
					if (destShards != null && !destShards.isEmpty() && !destShards.contains(to.getShard())) {
						logger.debug("Target shard {} not in destShards whitelist {}, skipping", to.getShard(), destShards);
						continue;
					}
					
					List<BsonDocument> hotChunks = getHotChunks(ns, from, to);
					
					if (from.getActiveChunks() == 1 && hotChunks.size() == 1) {
						BsonDocument chunkDoc = hotChunks.get(0);
						BsonValue id = chunkDoc.get("id");
						CountingMegachunk mega = innerMap.get(new BsonValueWrapper(id));
						logger.debug("splitting chunk: {}", mega);
						sourceShardClient.splitFind(ns, mega.getMin(), true);
						continue;
					}
					
					int i = 1;
					for (BsonDocument chunkDoc : hotChunks) {
	
						BsonValue id = chunkDoc.get("id");
						CountingMegachunk mega = innerMap.get(new BsonValueWrapper(id));
						BsonDocument min = mega.getMin();
						BsonDocument max = mega.getMax();
						
						boolean success = false;
						if (!balancerConfig.isDryRun()) {
							
							// Pre-emptive chunk splitting before move
							long maxDocs = balancerConfig.getMaxDocs();
							Document dataSize = sourceShardClient.dataSize(ns, min, max);
							if (dataSize != null) {
								Number countNumber = dataSize.get("numObjects", Number.class);
								if (countNumber != null) {
									long count = countNumber.longValue();
									
									int splitIteration = 0;
									while (count >= maxDocs) {
										logger.debug("maxDocs: {}, chunk too big, splitting - iteration {}", maxDocs, splitIteration);
										splitChunk(ns, min);
										chunkManager.loadChunkMap(ns, sourceChunksCache, chunkMap);
										
										// Recalculate size after split
										dataSize = sourceShardClient.dataSize(ns, min, max);
										if (dataSize == null) break;
										countNumber = dataSize.get("numObjects", Number.class);
										if (countNumber == null) break;
										count = countNumber.longValue();
										splitIteration++;
										
										// Safety break to avoid infinite loop
										if (splitIteration >= 10) {
											logger.warn("Max split iterations reached for chunk {}", mega);
											break;
										}
									}
								}
							}
							
							logger.debug("about to move chunk [ {} / {} ]: {}, _id: {}", i++, hotChunks.size(), mega, chunkDoc.get("_id"));
							success = moveChunkWithRetry(ns, mega, to.getShard(), 10);
							
							if (success) {
								moveCount++;
								mega.setShard(to.getShard());
								mega.updateLastMovedTime();
								balancerConfig.getStatsCollection().updateOne(
										and(eq("_id", chunkDoc.get("_id")), eq("chunks.id", id)),
										Updates.combine(
											Updates.set("chunks.$.balanced", true),
											Updates.inc("balancedChunks", 1)
										));
							}
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
	
	private void splitChunk(String ns, BsonDocument min) {
		Document result = sourceShardClient.splitFind(ns, min, true);
		logger.debug("splitFind / split chunk result: {}", result);
		
		BsonBinary uuidBinary = sourceShardClient.getUuidForNamespace(ns);
		
		BsonDocument chunkQuery = new BsonDocument("uuid", uuidBinary);
		chunkQuery.append("min", min);
//		RawBsonDocument newChunk = sourceShardClient.reloadChunk(chunkQuery);
//		if (newChunk == null) {
//			logger.debug("unable to reload chunk, query: {}", chunkQuery);
//		}
//		min = (BsonDocument) newChunk.get("min");
//		max = (BsonDocument) newChunk.get("max");
	}
	
	private boolean moveChunkWithRetry(String ns, CountingMegachunk mega, String toShard, int maxRetries) {
		for (int retry = 0; retry < maxRetries; retry++) {
			try {
				boolean success = sourceShardClient.moveChunk(ns, mega.getMin(), mega.getMax(), toShard, false, false, false, false, true);
				if (success) {
					logger.debug("moveChunk success on retry {}", retry);
					return true;
				}
			} catch (MongoCommandException mce) {
				if (mce.getMessage().contains("ChunkTooBig")) {
					// Extract maxDocs from error message using regex
					String message = mce.getMessage();
					java.util.regex.Pattern pattern = java.util.regex.Pattern.compile("maximum number of documents for a chunk is (\\d+)");
					java.util.regex.Matcher matcher = pattern.matcher(message);
					
					if (matcher.find()) {
						long extractedMaxDocs = Long.parseLong(matcher.group(1));
						logger.debug("ChunkTooBig error, extracted maxDocs: {}, splitting chunk", extractedMaxDocs);
						
						splitChunk(ns, mega.getMin());
						chunkManager.loadChunkMap(ns, sourceChunksCache, chunkMap);
						
						// Continue to retry with split chunk
						continue;
					} else {
						logger.warn("ChunkTooBig error but couldn't extract maxDocs from message: {}", message);
						break;
					}
				} else if (mce.getMessage().contains("no chunk found")) {
					logger.debug("No chunk found, reloading chunk map");
					chunkManager.loadChunkMap(ns, sourceChunksCache, chunkMap);
					continue;
				} else {
					logger.warn("Move chunk failed with error: {}", mce.getMessage());
					break;
				}
			} catch (Exception e) {
				logger.error("Unexpected error during moveChunk: {}", e.getMessage(), e);
				break;
			}
		}
		
		logger.warn("Failed to move chunk {} after {} retries", mega, maxRetries);
		return false;
	}

	private void updateChunkStats() {

		chunkStats = new ChunkStats();
		AggregateIterable<BsonDocument> results = balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(
						match(eq("analysisId", balancerConfig.getAnalysisId())),
						group(fields(eq("ns", "$ns"), eq("shard", "$shard")), Accumulators.sum("totalOps", "$total"),
								Accumulators.sum("activeChunks", "$activeChunks")),
						sort(orderBy(ascending("_id.ns"), descending("totalOps"))),
						project(fields(computed("ns", "$_id.ns"), computed("shard", "$_id.shard"), include("totalOps"),
								include("activeChunks")))));

		int resultNum = 0;
		for (BsonDocument result : results) {
			String ns = result.getString("ns").getValue();
			ChunkStatsEntry chunkStatsEntry = new ChunkStatsEntry(ns, result.getString("shard").getValue(),
					result.getNumber("totalOps").longValue(), result.getNumber("activeChunks").intValue());
			chunkStats.addEntry(chunkStatsEntry);
			resultNum++;
		}
		logger.debug("updateChunkStats(), got {} results from chunkStats collection, ns count: {}", resultNum, chunkStats.size());
	}
	
	private List<BsonDocument> getNamespaceStats() {

		List<BsonDocument> results = new ArrayList<>();
		
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
	
	private long minimumAbsoluteValue(Long a, Long b) {
		return (Math.abs(a) < Math.abs(b)) ? a : b;
	}

	private List<BsonDocument> getHotChunks(String ns, ChunkStatsEntry from, ChunkStatsEntry to) {
		
		List<BsonDocument> results = new ArrayList<>();
		
		long lowestDelta = minimumAbsoluteValue(from.getDeltaOps(), to.getDeltaOps());
		long target = Math.round(lowestDelta * 0.33);
		
		AggregateIterable<BsonDocument> resultsIterable = balancerConfig.getStatsCollection()
				.aggregate(Arrays.asList(
						match(and(eq("analysisId", balancerConfig.getAnalysisId()), eq("ns", ns), eq("shard", from.getShard()))),
						// project(fields(excludeId())),
						unwind("$chunks"),
						project(fields(computed("id", "$chunks.id"), computed("count", "$chunks.cnt"),
								computed("balanced", "$chunks.balanced"))),
						match(in("balanced", null, false)), sort(orderBy(descending("count")))
						));
		
		NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = chunkMap.get(ns);
		
		long totalOps = 0;
		for (BsonDocument result : resultsIterable) {
			
			BsonValue id = result.get("id");
			CountingMegachunk mega = innerMap.get(new BsonValueWrapper(id));
			
			Long elapsedSinceLastMove = mega.elapsedSinceLastMoved();
			if (elapsedSinceLastMove != null && elapsedSinceLastMove <= 60) {
				//logger.debug("skipping chunk from hot list, chunk was just moved {} minutes ago: {}", elapsedSinceLastMove, mega);
				continue;
			}
			
			int count = result.getInt64("count").intValue();
			
			if ((totalOps + count) >= target) {
				totalOps += count;
				results.add(result);
				break;
			} else {
				totalOps += count;
				results.add(result);
			}
			
		}
		
		logger.debug("{}: will move {} chunks from {} to {}", ns, results.size(), 
				from.getShard(), to.getShard());

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
		balancerConfig.setDryRun(config.getBoolean(DRY_RUN, false));
		balancerConfig.setDeltaThresholdPercent(config.getDouble(DELTA_THRESHOLD_PERCENT, 3.0));
		balancerConfig.setMoveCountBackoffThreshold(config.getInt(MOVE_COUNT_BACKOFF_THRESHOLD, 10));
		balancerConfig.setActiveChunkThreshold(config.getInt(ACTIVE_CHUNK_THRESHOLD, 10));
		balancerConfig.setMaxDocs(config.getLong(MAX_DOCS, 250000L));
		
		// Optional destShards configuration
		String[] destShards = config.getStringArray(DEST_SHARDS);
		if (destShards != null && destShards.length > 0) {
			balancerConfig.setDestShards(new HashSet<>(Arrays.asList(destShards)));
			logger.info("Restricting moves to destination shards: {}", Arrays.toString(destShards));
		}
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
		System.setProperty(ClassicConstants.CONFIG_FILE_PROPERTY, "shardbalancer_logback.xml");
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
		logger.debug("**** SHUTDOWN *****");
	}

}
