package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.corruptutil.DupeUtil;
import com.mongodb.dbhash.DbHashUtil;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardbalancer.CountingMegachunk;
import com.mongodb.shardsync.ChunkManager;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;
import com.mongodb.util.ProcessUtils;
import com.mongodb.util.ProcessUtils.ProcessInfo;
import com.mongodb.util.bson.BsonValueConverter;
import com.mongodb.util.bson.BsonValueWrapper;

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

	@Option(names = { "--includeNamespaces" }, description = "Namespaces to include", required = false, split = ",")
	private Set<String> includeNamespaceStrings;

	@Option(names = { "--shardMap" }, description = "Shard map, ex: shA|sh0,shB|sh1", required = false)
	private String shardMap;

	@Option(names = { "--wiredTigerConfigString" }, description = "WiredTiger config string", required = false)
	private String wiredTigerConfigString;

	@Option(names = {
			"--dupeCheckThreads" }, description = "# threads per collection to use for duplicate _id checking", required = false, defaultValue = "4")
	private int dupeCheckThreads;

	@Option(names = {
			"--archiveDbName" }, description = "database name that dupe checker uses to store found duplicates", required = false, defaultValue = "_dupesArchive")
	private String archiveDbName;

	@Option(names = {
			"--targetShards" }, description = "Target shards to distribute chunks to", required = false, split = ",")
	private Set<String> targetShards;
	
	@Option(names = { "--syncStartDelay" }, description = "Avoid long wait when resuming mongosync (for testing only)", required = false)
	private Integer syncStartDelay;

	private ShardConfigSync shardConfigSync;
	private SyncConfiguration shardConfigSyncConfig;
	private ChunkManager chunkManager;
	private ShardClient sourceShardClient;
	private ShardClient destShardClient;
	private DupeUtil dupeUtil;
	
	private final Map<String, RawBsonDocument> destChunksCache = new LinkedHashMap<>();
	private final Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> destChunkMap = new HashMap<>();
	private Map<String, Document> collectionsMap;

	private List<Namespace> includeNamespaces = new ArrayList<>();

	List<MongoSyncRunner> mongosyncRunners;

	private AtomicInteger mongosyncRunnersPausedCount = new AtomicInteger(0);
	
	private Random random = new Random();

	private void initialize() throws IOException {

		shardConfigSyncConfig = new SyncConfiguration();
		shardConfigSyncConfig.setSourceClusterUri(sourceUri);
		shardConfigSyncConfig.setDestClusterUri(destUri);
		if (includeNamespaceStrings != null) {
			shardConfigSyncConfig.setNamespaceFilters(includeNamespaceStrings.toArray(new String[0]));
		}

		if (shardMap != null) {
			shardConfigSyncConfig.setShardMap(shardMap.split(","));
		}

		if (wiredTigerConfigString != null) {
			shardConfigSyncConfig.setWiredTigerConfigString(wiredTigerConfigString);
		}

		if (targetShards != null && !targetShards.isEmpty()) {
			shardConfigSyncConfig.setTargetShards(targetShards);
		}

		chunkManager = new ChunkManager(shardConfigSyncConfig);
		chunkManager.setWaitForDelete(true);
		chunkManager.initalize();
		this.sourceShardClient = shardConfigSyncConfig.getSourceShardClient();
		this.destShardClient = shardConfigSyncConfig.getDestShardClient();

		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap(includeNamespaceStrings);
		
		destShardClient.populateShardMongoClients();
		destShardClient.stopBalancer();
		
		this.collectionsMap = sourceShardClient.getCollectionsMap();

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

		dupeUtil = new DupeUtil(sourceUri, destUri, archiveDbName, null);
		dupeUtil.setThreads(dupeCheckThreads);
		dupeUtil.addFilters(includeNamespaceStrings.toArray(new String[0]));
		dupeUtil.setDropArchiveDb(drop);
		dupeUtil.initialize();

		if (includeNamespaceStrings != null && !includeNamespaceStrings.isEmpty()) {
			for (String ns : includeNamespaceStrings) {
				includeNamespaces.add(new Namespace(ns));
			}
		}
	}

	@Override
	public Integer call() throws Exception {
		initialize();

		if (dupeCheck) {
			dupeUtil.run();
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
			mongosync.setSyncStartDelay(syncStartDelay);
			mongosync.setBuildIndexes(buildIndexes);
			mongosync.setLogDir(logDir);
			mongosync.setIncludeNamespaces(includeNamespaces);
			if (i == 0) {
				mongosync.setCoordinator(true);
			}

			String destShardId = chunkManager.getShardMapping(source.getId());
			Shard dest = destShardClient.getShardsMap().get(destShardId);
			if (dest != null) {
				logger.debug(String.format("Creating MongoSyncRunner for %s ==> %s", source.getId(), dest.getId()));
			}
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

		if (targetShards == null || targetShards.isEmpty()) {
			DbHashUtil dbHash = new DbHashUtil(chunkManager, includeNamespaceStrings);
			dbHash.call();
		} else {
			logger.debug(
					"skipping dbHash since targetShards were specified / shard alignment does not match, reverting to estimated document counts");
			restoreDuplicatesOnTarget();
			compareCollectionCounts();
		}

		return 0;
	}

	private void restoreDuplicatesOnTarget() {
		
		handleDuplicatesBeforeRestore();
		
		for (Namespace ns : includeNamespaces) {
			Namespace archiveNs1 = new Namespace(archiveDbName, ns.getNamespace() + "_1");
			Namespace archiveNs2 = new Namespace(archiveDbName, ns.getNamespace() + "_2");

			// Archive is stored on the target side, but it's the "source" when copying the
			// dupes back into the target
			MongoCollection<Document> source1 = destShardClient.getCollection(archiveNs1);
			MongoCollection<Document> source2 = destShardClient.getCollection(archiveNs2);
			MongoCollection<Document> target = destShardClient.getCollection(ns);

			// Process documents from source1
			processSourceCollection(source1, target);

			// Process documents from source2
			processSourceCollection(source2, target);
		}
	}

	/**
	 * Processes a source collection and inserts documents into target in batches
	 * 
	 * @param sourceCollection The source collection to read from
	 * @param targetCollection The target collection to insert into
	 */
	private void processSourceCollection(MongoCollection<Document> sourceCollection,
			MongoCollection<Document> targetCollection) {
		// Define batch size
		final int BATCH_SIZE = 1000;
		List<Document> batch = new ArrayList<>(BATCH_SIZE);

		// Retrieve all documents from source
		try (MongoCursor<Document> cursor = sourceCollection.find().iterator()) {
			while (cursor.hasNext()) {
				batch.add(cursor.next());

				// When batch is full, insert it
				if (batch.size() >= BATCH_SIZE) {
					insertBatchWithDuplicateHandling(batch, targetCollection);
					batch.clear();
				}
			}

			// Insert any remaining documents
			if (!batch.isEmpty()) {
				insertBatchWithDuplicateHandling(batch, targetCollection);
			}
		}
	}

	/**
	 * Inserts a batch of documents, handling duplicate key exceptions
	 * 
	 * @param batch            The batch of documents to insert
	 * @param targetCollection The target collection to insert into
	 */
	private void insertBatchWithDuplicateHandling(List<Document> batch, MongoCollection<Document> targetCollection) {
		try {
			// Try bulk insert
			targetCollection.insertMany(batch, new InsertManyOptions().ordered(false));
		} catch (MongoBulkWriteException e) {
			// Handle duplicate key exceptions
			handleDuplicateKeyErrors(e, batch, targetCollection);
		}
	}

	/**
	 * Handles duplicate key exceptions during batch inserts
	 * 
	 * @param e                The exception that was thrown
	 * @param batch            The batch that was being inserted
	 * @param targetCollection The target collection
	 */
	private void handleDuplicateKeyErrors(MongoBulkWriteException e, List<Document> batch,
			MongoCollection<Document> targetCollection) {
		// Get the write errors from the exception
		List<BulkWriteError> writeErrors = e.getWriteErrors();

		// Log information about the errors
		for (BulkWriteError error : writeErrors) {
			if (error.getCode() == 11000) { // 11000 is the error code for duplicate key
				// Log duplicate key error
				logger.info("Duplicate key error for document with index: " + error.getIndex());

				// Here you could implement special handling for duplicates if needed
				// For example, you might want to update the existing document instead of
				// inserting

				// Example stub for future implementation:
				// Document duplicateDoc = batch.get(error.getIndex());
				// handleDuplicateDocument(duplicateDoc, targetCollection);
			} else {
				// Log other types of errors
				logger.error("Non-duplicate error: " + error.getMessage());
			}
		}

		// Log the successful insertions count
		logger.info(
				"Successfully inserted " + e.getWriteResult().getInsertedCount() + " documents out of " + batch.size());
	}

	/**
	 * Future method to handle duplicate documents according to business rules
	 * 
	 * @param duplicateDoc     The document that caused a duplicate key error
	 * @param targetCollection The target collection
	 */
	private void handleDuplicateDocument(Document duplicateDoc, MongoCollection<Document> targetCollection) {
		// This method would be implemented according to specific business rules
		// For example:
		// 1. You might want to update the existing document
		// 2. You might want to merge the duplicate with the existing document
		// 3. You might want to rename and insert the duplicate
		// 4. You might want to log it for manual review

		// Stub implementation:
		Object id = duplicateDoc.get("_id");
		logger.info("Handling duplicate document with _id: " + id);

		// Example of one approach: update the existing document if newer
		// if (duplicateDoc.containsKey("lastModified")) {
		// Date lastModified = duplicateDoc.getDate("lastModified");
		// targetCollection.updateOne(
		// Filters.and(
		// Filters.eq("_id", id),
		// Filters.lt("lastModified", lastModified)
		// ),
		// new Document("$set", duplicateDoc)
		// );
		// }
	}

	private void deleteDuplicatesOnSource() {
		for (Namespace ns : includeNamespaces) {
			Namespace archiveNs1 = new Namespace(archiveDbName, ns.getNamespace() + "_1");
			Namespace archiveNs2 = new Namespace(archiveDbName, ns.getNamespace() + "_2");
			MongoCollection<Document> c1 = destShardClient.getCollection(archiveNs1);
			MongoCollection<Document> c2 = destShardClient.getCollection(archiveNs2);

			deleteDuplicates(c1, c2, ns);
		}
	}

	private void deleteDuplicates(MongoCollection<Document> archive1, MongoCollection<Document> archive2, Namespace sourceNs) {
		MongoCollection<Document> sourceColl = sourceShardClient.getCollection(sourceNs);

		List<Object> idBatch = new ArrayList<>(1000);
		int totalDeleted = 0;
		int batchCount = 0;

		logger.info("Starting deletion of duplicates from {} based on archive collection", sourceNs);

		Document collMeta = collectionsMap.get(sourceNs.getNamespace());
		
		// if there's no collection metadata, it's most likely unsharded
		if (collMeta == null) {
			throw new IllegalArgumentException(String.format("collectionsMap does not contain namespace %s", sourceNs));
		}
		Document shardKeyMetaDoc = (Document)collMeta.get("key");
		Set<String> shardKey = shardKeyMetaDoc.keySet();
		
		
		Bson projection = Projections.fields(Projections.include("_id"));
		for (String field : shardKey) {
		    projection = Projections.fields(projection, Projections.include(field));
		}
		
		FindIterable<Document> docs1 = archive1.find().projection(projection).sort(Sorts.ascending("_id"));
		FindIterable<Document> docs2 = archive2.find().projection(projection).sort(Sorts.ascending("_id"));
		MongoCursor<Document> docs2Cursor = docs2.iterator();

		for (Document d : docs1) {
			Document d2 = docs2Cursor.next();
			
//			BsonValueWrapper w1 = getShardKeyWrapper(shardKey, d);
//			BsonValueWrapper w2 = getShardKeyWrapper(shardKey, d2);
//			NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = destChunkMap.get(sourceNs.getNamespace());
//			if (innerMap == null) {
//				logger.warn("innerMap was null");
//			}
//			
//			Map.Entry<BsonValueWrapper, CountingMegachunk> e1 = innerMap.floorEntry(w1);
//			Map.Entry<BsonValueWrapper, CountingMegachunk> e2 = innerMap.floorEntry(w2);
//			
//			CountingMegachunk c1 = e1.getValue();
//			CountingMegachunk c2 = e2.getValue();
//			
//			String s1 = c1.getShard();
//			String s2 = c2.getShard();
//			
//			
//			if (c1.equals(c2)) {
//				logger.error("Duplicates were found in the same chunk for _id: {} and _id: {}, ns: {}");
//				throw new RuntimeException("duplicates were found on the same chunk");
//			}
//			
//			if (s1.equals(s2)) {
//				String newShard = getRandomShardExcept(s2);
//				logger.debug("duplicates are on the same shard, going to move chunk with min: {}, max: {}, from: {}, to: {}", c2.getMin(), c2.getMax(), s1, newShard);
//				destShardClient.moveChunk(sourceNs.getNamespace(), c2.getMin(), c2.getMax(), newShard, false, false, true, false);
//			}
			
			idBatch.add(d.get("_id"));

			// When we reach 1000 ids, execute a batch delete
			if (idBatch.size() >= 1000) {
				int deleted = deleteIdsInBatchWithRetry(sourceColl, idBatch);
				totalDeleted += deleted;
				batchCount++;

				logger.info("Batch #{}: Deleted {} documents from {}", batchCount, deleted, sourceNs);
				idBatch.clear();
			}
		}

		// Process any remaining ids
		if (!idBatch.isEmpty()) {
			int deleted = deleteIdsInBatchWithRetry(sourceColl, idBatch);
			totalDeleted += deleted;
			batchCount++;

			logger.info("Final batch #{}: Deleted {} documents from {}", batchCount, deleted, sourceNs);
		}

		logger.info("Completed deletion process. Total documents deleted: {} in {} batches", totalDeleted, batchCount);
	}
	
	private String getRandomShardExcept(String s2) {
	    // Create a list from the set, excluding s2
	    List<String> availableShards = new ArrayList<>();
	    for (String shard : targetShards) {
	        if (!shard.equals(s2)) {
	            availableShards.add(shard);
	        }
	    }
	    
	    // Check if we have any shards left after exclusion
	    if (availableShards.isEmpty()) {
	        return null; // or throw an exception
	    }
	    
	    int randomIndex = random.nextInt(availableShards.size());
	    return availableShards.get(randomIndex);
	}

	private int deleteIdsInBatchWithRetry(MongoCollection<Document> collection, List<Object> ids) {
		final int MAX_RETRIES = 3;
		final int RETRY_DELAY_MS = 1000; // Initial delay of 1 second

		int retryCount = 0;
		Exception lastException = null;

		while (retryCount < MAX_RETRIES) {
			try {
				return deleteIdsInBatch(collection, new ArrayList<>(ids)); // Create a defensive copy
			} catch (MongoException e) {
				lastException = e;
				retryCount++;

				if (retryCount >= MAX_RETRIES) {
					logger.error("Failed to delete batch after {} retries", MAX_RETRIES, e);
					break;
				}

				// Exponential backoff
				int delayMs = RETRY_DELAY_MS * (int) Math.pow(2, retryCount - 1);
				logger.warn("Batch deletion failed (attempt {}/{}). Retrying in {} ms. Error: {}", retryCount,
						MAX_RETRIES, delayMs, e.getMessage());

				try {
					Thread.sleep(delayMs);
				} catch (InterruptedException ie) {
					Thread.currentThread().interrupt();
					throw new RuntimeException("Interrupted during retry delay", ie);
				}
			}
		}

		// If we've exhausted retries, throw the last exception
		if (lastException != null) {
			throw new RuntimeException("Failed to delete batch after exhausting retries", lastException);
		}

		return 0; // Should never reach here
	}

	// Original helper to perform the actual deletion
	private int deleteIdsInBatch(MongoCollection<Document> collection, List<Object> ids) {
		Bson filter = Filters.in("_id", ids);
		DeleteResult result = collection.deleteMany(filter);
		return (int) result.getDeletedCount();
	}
	
	private void handleDuplicatesBeforeRestore() {
	    // Initialize the duplicate resolver
	    DuplicateResolver resolver = new DuplicateResolver(destShardClient);
	    
	    // For each namespace with duplicates
	    for (Namespace ns : includeNamespaces) {
	        Namespace archiveNs1 = new Namespace(archiveDbName, ns.getNamespace() + "_1");
	        Namespace archiveNs2 = new Namespace(archiveDbName, ns.getNamespace() + "_2");

	        // Get the MongoDB collections
	        MongoCollection<Document> archive1 = destShardClient.getCollection(archiveNs1);
	        MongoCollection<Document> archive2 = destShardClient.getCollection(archiveNs2);
	        
	        // Get the shard key for this namespace
	        Document collMeta = collectionsMap.get(ns.getNamespace());
	        if (collMeta == null) {
	            logger.warn("No collection metadata found for {}, skipping duplicate handling", ns);
	            continue;
	        }
	        
	        Document shardKeyMetaDoc = (Document)collMeta.get("key");
	        Set<String> shardKey = shardKeyMetaDoc.keySet();
	        
	        // Load the duplicates
	        List<Document> duplicates1 = new ArrayList<>();
	        List<Document> duplicates2 = new ArrayList<>();
	        
	        // Create projections to get _id and shard key fields
	        Bson projection = Projections.fields(Projections.include("_id"));
	        for (String field : shardKey) {
	            projection = Projections.fields(projection, Projections.include(field));
	        }
	        
	        // Load documents from archive collections
	        archive1.find().projection(projection).into(duplicates1);
	        archive2.find().projection(projection).into(duplicates2);
	        
	        logger.info("Found {} and {} duplicates in archive collections for {}", 
	                  duplicates1.size(), duplicates2.size(), ns);
	        
	        // Get the chunk map for this namespace
	        NavigableMap<BsonValueWrapper, CountingMegachunk> chunkMap = destChunkMap.get(ns.getNamespace());
	        if (chunkMap == null) {
	            logger.warn("No chunk map found for {}, skipping duplicate handling", ns);
	            continue;
	        }
	        
	        // Build the duplicate mappings using the actual shard key
	        resolver.buildDuplicateMapping(ns.getNamespace(), duplicates1, chunkMap, shardKey);
	        resolver.buildDuplicateMapping(ns.getNamespace(), duplicates2, chunkMap, shardKey);
	        
	        // Determine split points based on duplicate distribution
	        resolver.determineSplitPoints(ns.getNamespace());
	    }
	    
	    // Execute the splits and migrations
	    resolver.executeSplitsAndMigrations();
	}

	private void compareCollectionCounts() {
		for (Namespace ns : includeNamespaces) {
			Number sourceCount = sourceShardClient.getFastCollectionCount(ns.getDatabaseName(), ns.getCollectionName());
			Number destCount = destShardClient.getFastCollectionCount(ns.getDatabaseName(), ns.getCollectionName());
			if (sourceCount.equals(destCount)) {
				logger.debug("    ns: {}, sourceCount and destCount match {} -- ✅ PASS", ns, sourceCount);
			} else {
				logger.debug("    ns: {}, sourceCount and destCount differ, sourceCount: {}, destCount: {} -- ❌ FAIL",
						ns, sourceCount, destCount);
			}
		}
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
				logger.debug("includeNamespaces: {}", includeNamespaceStrings);

				shardConfigSync.syncMetadataOptimized();
				chunkManager.loadChunkMap(destShardClient, null, destChunksCache, destChunkMap);
				deleteDuplicatesOnSource();

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

			// MongoSyncStatus status = runner.checkStatus();
			runner.resume();
			runner.waitForRunningState();

		} else {
			logger.warn("more than 1 mongosync appears to have been paused, current paused runner: {}", runner.getId());
		}
	}

}