package com.mongodb.shardbalancer;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DefaultListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	
	private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");


	private final static String SOURCE_URI = "source";
	private final static String SOURCE_SHARDS = "sourceShards";
	private final static String DEST_SHARDS = "destShards";
	private final static String INCLUDE_NAMESPACES = "includeNamespaces";
	private final static String DRY_RUN = "dryRun";
	private final static String LIMIT = "limit";
	private final static String START_TIME = "startTime";
	private final static String END_TIME = "endTime";

	private BalancerConfig balancerConfig;

	private ShardClient sourceShardClient;

	private ChunkManager chunkManager;

	private int limit;

	Map<String, RawBsonDocument> sourceChunksCache;

	private LocalTime startTime;
	private LocalTime endTime;
	
	private ZonedDateTime startDateTime;
    private ZonedDateTime endDateTime;
    
    private Iterator<RawBsonDocument> chunkIterator;
    private boolean firstRun = true;
    
    private double maxChunkSize = 1073741824;
    private Map<String, Double> collStatsMap = new HashMap<>();

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

	private void balanceData() {
		logger.debug("Starting balanceData(), will run until endTime: {}", endTime);
		int moveCount = 0;
		//MongoCollection<RawBsonDocument> chunksColl = sourceShardClient.getChunksCollectionRaw();
		
		if (firstRun) {
            chunkIterator = sourceChunksCache.values().iterator();
            firstRun = false;
        }

		while (chunkIterator.hasNext() && moveCount < limit) {
			
			if (ZonedDateTime.now(ZoneId.of("UTC")).isAfter(endDateTime)) {
                logger.debug("End time {} reached, terminating the process for today", endDateTime);
                return;
            }
			
			RawBsonDocument chunk = chunkIterator.next();
			//String currentShard = chunk.getString("shard").getValue();
			//int rangesMoved = 0;

			//while (moveCount < limit) {
				// Check if endTime is reached
				

				String destShard = RandomUtils.getRandomElementFromSet(balancerConfig.getDestShards());

//				if (currentShard.equals(destShard)) {
//					logger.debug("currentShard == destShard, all chunks moved");
//					break;
//				}
				
				BsonString bsonNs = chunk.getString("ns");
				String ns = bsonNs.getValue();
				BsonDocument min = (BsonDocument) chunk.get("min");
				BsonObjectId id = chunk.getObjectId("_id");
				BsonDocument max = (BsonDocument) chunk.get("max");
				//sourceShardClient.moveRange(ns, min, destShard, balancerConfig.isDryRun());
				
//				Double stats = null;
//				if (collStatsMap.containsKey(ns)) {
//					stats = collStatsMap.get(ns);
//				} else {
//					Document statsDoc = sourceShardClient.collStats(ns);
//					stats = statsDoc.getDouble("avgObjSize");
//					collStatsMap.put(ns, stats);
//				}
//				
//				
//				Double maxDocs = Double.valueOf(2.0 * (maxChunkSize / stats));
//				logger.debug("maxDocs: {}", maxDocs);
				
				Document dataSize = sourceShardClient.dataSize(ns, min, max);
				long count = dataSize.getLong("numObjects");
				
				int i = 0;
				while (count >= 3172058) {
					//logger.debug("maxDocs: {}, chunk too big, splitting", maxDocs);
					
					logger.debug("chunk too big, splitting - iteration {}", i);
					sourceShardClient.splitFind(ns, min, true);
					
					BsonBinary uuidBinary = sourceShardClient.getUuidForNamespace(ns);
					
					BsonDocument chunkQuery = new BsonDocument("uuid", uuidBinary);
					chunkQuery.append("min", min);
					RawBsonDocument newChunk = sourceShardClient.reloadChunk(chunkQuery);
					if (newChunk == null) {
						logger.debug("unable to reload chunk, query: {}", chunkQuery);
						continue;
					}
					min = (BsonDocument) newChunk.get("min");
					id = newChunk.getObjectId("_id");
					max = (BsonDocument) newChunk.get("max");
					
					dataSize = sourceShardClient.dataSize(ns, min, max);
					count = dataSize.getLong("numObjects");
					i++;
				}
				
				boolean result = sourceShardClient.moveChunk(ns, min, max, destShard, false, false, true, false);
				
				if (result) {
					moveCount++;
					logger.debug(
							"{}: moved range with min: {}, max: {} to shard {}, totalMoved: {} - _id: {}",
							ns, min, max, destShard, moveCount, id);
				}
				
				//rangesMoved++;
				

//				Bson filter = and(eq("uuid", chunk.get("uuid")), eq("min", min));
//				RawBsonDocument ch = chunksColl.find(filter).first();
//				currentShard = ch.getString("shard").getValue();
			//}
		}
	}

	public Integer call() throws ConfigurationException, InterruptedException {

		parseArgs();
		init();
		
		while (true) {
            // Get current date and time in UTC
            ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));

            // Calculate start and end times for today
            startDateTime = now.with(startTime);
            endDateTime = startDateTime.with(endTime);

            // Move to the next day if the end time is before the start time
            if (endDateTime.isBefore(startDateTime)) {
            	logger.debug("endDateTime is before startDateTime, adding 1 day to endDateTime");
                endDateTime = endDateTime.plusDays(1); 
            }
            
            if (now.isAfter(endDateTime)) {
            	logger.debug("Current time is after endDateTime, adding 1 day to both start/end times");
                startDateTime = startDateTime.plusDays(1);
                endDateTime = endDateTime.plusDays(1);
            }

            // If current time is within the start and end time window, start immediately
            if (now.isAfter(startDateTime) && now.isBefore(endDateTime)) {
            	logger.debug("Already witin time window, starting balanceData()");
                balanceData();
            } else {
                // Wait until the start time
                waitUntil();
                balanceData();
            }
            
            if (chunkIterator != null && !chunkIterator.hasNext()) {
            	logger.debug("All chunks have been iterated -- all done!");
            	break;
            }

            startDateTime = startDateTime.plusDays(1);
            // Wait until the next start time
            waitUntil();
        }
		return 0;
	}

	private static LocalTime parseTime(String timeString) {
		try {
			return LocalTime.parse(timeString, TIME_FORMATTER);
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid time format. Please use 'HH:MM' format.", e);
		}
	}
	
	private void waitUntil() {
		logger.debug("Sleeping until startDateTime: {}", startDateTime);
        ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
        if (now.isBefore(startDateTime)) {
            long millisUntilStart = ChronoUnit.MILLIS.between(now, startDateTime);
            try {
                Thread.sleep(millisUntilStart);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

	protected void parseArgs() throws ConfigurationException {

		Configuration config = readProperties();
		this.balancerConfig = new BalancerConfig();
		balancerConfig.setSourceClusterUri(config.getString(SOURCE_URI));
		String[] includes = config.getStringArray(INCLUDE_NAMESPACES);
		balancerConfig.setNamespaceFilters(includes);

		String[] sourceShards = config.getStringArray(SOURCE_SHARDS);
		balancerConfig.setSourceShards(new HashSet<>(Arrays.asList(sourceShards)));

		String[] destShards = config.getStringArray(DEST_SHARDS);
		balancerConfig.setDestShards(new HashSet<>(Arrays.asList(destShards)));

		balancerConfig.setDryRun(config.getBoolean(DRY_RUN, false));

		this.limit = config.getInt(LIMIT, Integer.MAX_VALUE);
		
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		startTime = parseTime(config.getString(START_TIME));
        endTime = parseTime(config.getString(END_TIME));
        
        // Ensure endTime is after startTime, or handle the case where it spans to the next day
        if (endTime.isBefore(startTime)) {
            endTime = endTime.plusHours(24);
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
