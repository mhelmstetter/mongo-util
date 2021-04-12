package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.regex;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.exec.ExecuteException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.model.IndexSpec;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.model.ShardCollection;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.mongomirror.MongoMirrorRunner;
import com.mongodb.mongomirror.MongoMirrorStatus;
import com.mongodb.mongomirror.MongoMirrorStatusInitialSync;
import com.mongodb.mongomirror.MongoMirrorStatusOplogSync;

import picocli.CommandLine.Command;

@Command(name = "shardSync", mixinStandardHelpOptions = true, version = "shardSync 1.0")
public class ShardConfigSync implements Callable<Integer> {
	
	private final static DocumentCodec codec = new DocumentCodec();

	DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmm_ss");

	private static Logger logger = LoggerFactory.getLogger(ShardConfigSync.class);

	private final static int BATCH_SIZE = 512;
	
	public final static int SECONDS_IN_YEAR = 31536000;

	private final static Document LOCALE_SIMPLE = new Document("locale", "simple");

	private String sourceClusterUri;

	private String destClusterUri;
	
	private String sourceClusterPattern;
	private String destClusterPattern;
	
	private String sourceRsPattern;
	private String destRsPattern;
	
	private String destCsrsUri;

	private boolean dropDestDbs;
	private boolean dropDestDbsAndConfigMetadata;
	private boolean nonPrivilegedMode = false;
	private boolean doChunkCounts;
	private boolean preserveUUIDs;
	private String compressors;
	private String oplogBasePath;
	private String bookmarkFilePrefix;
	private boolean reverseSync;
	private boolean skipBuildIndexes;
	private Integer collStatsThreshold;
	private boolean dryRun;
	private boolean shardToRs;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;

	private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();
	private Map<String, String> destToSourceShardMap = new HashMap<String, String>();
	
	private Map<String, String> altSourceToDestShardMap = new HashMap<String, String>();

	private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
	private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();

	private boolean filtered = false;

	private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	private Set<String> includeDatabases = new HashSet<String>();

	// ugly, but we need a set of includeDatabases that we pass to mongomirror
	// vs. the includes that we use elsewhere
	private Set<String> includeDatabasesAll = new HashSet<String>();

	private String[] shardMap;

	private File mongomirrorBinary;

	private long sleepMillis;

	private String numParallelCollections;
	private int mongoMirrorStartPort = 9001;

	private String writeConcern;

	private Long cleanupOrphansSleepMillis;

	private String destVersion;
	private List<Integer> destVersionArray;

	private boolean sslAllowInvalidHostnames;
	private boolean sslAllowInvalidCertificates;
	
	private boolean skipFlushRouterConfig;

	CodecRegistry registry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(),
			fromProviders(PojoCodecProvider.builder().automatic(true).build()));

	DocumentCodec documentCodec = new DocumentCodec(registry);

	public ShardConfigSync() {
		logger.debug("ShardConfigSync starting");
	}
	
	@Override
    public Integer call() throws Exception {
		return 0;
	}

	@SuppressWarnings("unchecked")
	public void initializeShardMappings() {
		logger.debug("Start initializeShardMappings()");
		
		String source = sourceClusterUri == null ? sourceClusterPattern : sourceClusterUri;
		String dest = destClusterUri == null ? destClusterPattern : destClusterUri;
		
		
		if (this.shardMap != null) {
			// shardMap is for doing an uneven shard mapping, e.g. 10 shards on source
			// down to 5 shards on destination
			logger.debug("Custom n:m shard mapping");
			
			for (String mapping : shardMap) {
				String[] mappings = mapping.split("\\|");
				logger.debug(mappings[0] + " ==> " + mappings[1]);
				sourceToDestShardMap.put(mappings[0], mappings[1]);
			}
			
			
			sourceShardClient = new ShardClient("source", source, sourceToDestShardMap.keySet());
			destShardClient = new ShardClient("dest", dest, sourceToDestShardMap.values());
			sourceShardClient.setRsPattern(sourceRsPattern);
			destShardClient.setRsPattern(destRsPattern);
			destShardClient.setCsrsUri(destCsrsUri);
			
			sourceShardClient.init();
			destShardClient.init();
			
		} else {
			logger.debug("Default 1:1 shard mapping");
			
			sourceShardClient = new ShardClient("source", source, null);
			destShardClient = new ShardClient("dest", dest, null);
			sourceShardClient.setRsPattern(sourceRsPattern);
			destShardClient.setRsPattern(destRsPattern);
			destShardClient.setCsrsUri(destCsrsUri);
			
			sourceShardClient.init();
			destShardClient.init();
			checkDestShardClientIsMongos();
			
			logger.debug("Source shard count: " + sourceShardClient.getShardsMap().size());
			// default, just match up the shards 1:1
			int index = 0;
			
			Map<String, Shard> sourceTertiaryMap = sourceShardClient.getTertiaryShardsMap();
			
			//Map<String, Shard> sourceShardsMap = sourceTertiaryMap.isEmpty() ?  sourceShardClient.getShardsMap() : sourceTertiaryMap;
			Map<String, Shard> sourceShardsMap = sourceShardClient.getShardsMap();
			
			List<Shard> destList = new ArrayList<Shard>(destShardClient.getShardsMap().values());
			
			if (shardMap == null && sourceShardsMap.size() != destList.size()) {
				throw new IllegalArgumentException(String.format("disparate shard counts requires shardMap to be defined, sourceShardCount: %s, destShardCount: %s", 
						sourceShardsMap.size(), destList.size()));
			}
			
			if (! shardToRs) {
				for (Iterator<Shard> i = sourceShardsMap.values().iterator(); i.hasNext();) {
					Shard sourceShard = i.next();
					Shard destShard = destList.get(index);
					if (destShard != null) {
						logger.debug(sourceShard.getId() + " ==> " + destShard.getId());
						sourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
					}
					index++;
				}
				
				index = 0;
				for (Iterator<Shard> i = sourceTertiaryMap.values().iterator(); i.hasNext();) {
					Shard sourceShard = i.next();
					Shard destShard = destList.get(index);
					if (destShard != null) {
						logger.debug("altMapping: " + sourceShard.getId() + " ==> " + destShard.getId());
						altSourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
					}
					index++;
				}
			}
			
			
			
		}
		// reverse map
		destToSourceShardMap = MapUtils.invertMap(sourceToDestShardMap);
		
		if (! sourceShardClient.isMongos()) {
			throw new IllegalArgumentException("source connection must be to a mongos router");
		}
		
		checkDestShardClientIsMongos();
		
	}
	
	private void checkDestShardClientIsMongos() {
		if (!destShardClient.isMongos() && !shardToRs) {
			throw new IllegalArgumentException("dest connection must be to a mongos router unless using shardToRs");
		}
	}

	public void shardCollections() {
		logger.debug("Starting shardCollections");
		sourceShardClient.populateCollectionsMap();
		enableDestinationSharding();
		shardDestinationCollections();
	}
	
	private boolean filterCheck(String nsStr) {
		Namespace ns = new Namespace(nsStr);
		return filterCheck(ns);
	}
	
	private boolean filterCheck(Namespace ns) {
		if (filtered && !includeNamespaces.contains(ns) && !includeDatabases.contains(ns.getDatabaseName())) {
			logger.trace("Namespace " + ns + " filtered, skipping");
			return true;
		}
		if (ns.getDatabaseName().equals("config") || ns.getDatabaseName().equals("admin")) {
			return true;
		}
		return false;
	}
	
	private Map<Namespace, Set<IndexSpec>> getIndexSpecs(MongoClient client, Set<String> filterSet) {
		Map<Namespace, Set<IndexSpec>> sourceIndexSpecs = new LinkedHashMap<>();
		for (String dbName : client.listDatabaseNames()) {
			MongoDatabase sourceDb = client.getDatabase(dbName);
			for (String collectionName : sourceDb.listCollectionNames()) {
				Namespace ns = new Namespace(dbName, collectionName);
				if (filterCheck(ns) || (filterSet != null && ! filterSet.contains(ns.getNamespace()))) {
					continue;
				}
				
				Set<IndexSpec> indexSpecs = new HashSet<>();
				sourceIndexSpecs.put(ns, indexSpecs);
				MongoCollection<RawBsonDocument> collection = sourceDb.getCollection(collectionName, RawBsonDocument.class);
				for (RawBsonDocument sourceSpec : collection.listIndexes(RawBsonDocument.class)) {
					indexSpecs.add(IndexSpec.fromDocument(sourceSpec));
				}
			}
		}
		return sourceIndexSpecs;
	}
	
	public void syncIndexesShards(boolean createMissing, boolean extendTtl) {
		logger.debug(String.format("Starting syncIndexes: extendTtl: %s", extendTtl));
		destShardClient.populateShardMongoClients();
		sourceShardClient.populateShardMongoClients();
		
		//Map<String, Document> map = sourceShardClient.getCollectionsMap();
		//Set<String> filterSet = Collections.emptySet();
		
		Map<Namespace, Set<IndexSpec>> sourceIndexSpecs = getIndexSpecs(sourceShardClient.getMongoClient(), null);
		Map<String, Map<Namespace, Set<IndexSpec>>> destShardsIndexSpecs = new HashMap<>();
		
		// TODO fix for shard to rs
		for (Map.Entry<String, MongoClient> entry : destShardClient.getShardMongoClients().entrySet()) {
			String shardName = entry.getKey();
			MongoClient destClient = entry.getValue();
			Map<Namespace, Set<IndexSpec>> destShardIndexSpecs = getIndexSpecs(destClient, null);
			destShardsIndexSpecs.put(shardName, destShardIndexSpecs);
		}
		
		for (Map.Entry<Namespace, Set<IndexSpec>> sourceEntry : sourceIndexSpecs.entrySet()) {
            Namespace ns = sourceEntry.getKey();
            Set<IndexSpec> sourceSpecs = sourceEntry.getValue();
            
        	for (Map.Entry<String, Map<Namespace, Set<IndexSpec>>> entry : destShardsIndexSpecs.entrySet()) {
            	String shardName = entry.getKey();
            	Map<Namespace, Set<IndexSpec>> shardIndexSpecsMap = entry.getValue();
            	Set<IndexSpec> destSpec = shardIndexSpecsMap.get(ns);
            	
            	if (destSpec == null || sourceSpecs == null) {
            		continue;
            	}
            	Set<IndexSpec> diff = Sets.difference(sourceSpecs, destSpec);
            	
            	if (diff.isEmpty()) {
            		logger.debug(String.format("%s - all indexes match for shard %s, indexCount: %s", ns, shardName, sourceSpecs.size()));
            		
            		if (extendTtl) {
            			destShardClient.extendTtls(shardName, ns, sourceSpecs);
            		}
            		
            	} else {
            		if (createMissing) {
            			logger.debug(String.format("%s - missing dest indexes %s missing on shard %s, creating", ns, diff, shardName));
                		destShardClient.createIndexes(shardName, ns, diff, extendTtl);
            		} else {
            			logger.debug(String.format("%s - missing dest indexes %s missing on shard %s", ns, diff, shardName));
            		}
            		
            	}

            }
        }
	}
	
	public void migrateMetadata() throws InterruptedException {
		migrateMetadata(true, true);
	}

	public void migrateMetadata(boolean enableDestinationSharding, boolean compareAndMove) throws InterruptedException {
		logger.debug(String.format("Starting metadata sync/migration, %s: %s", 
				ShardConfigSyncApp.NON_PRIVILEGED, nonPrivilegedMode));

		stopBalancers();
		// checkAutosplit();
		
		if (enableDestinationSharding) {
			enableDestinationSharding();
		}

		sourceShardClient.populateCollectionsMap();
		shardDestinationCollections();

		if (nonPrivilegedMode) {
			createDestChunksUsingSplitCommand();
		} else {
			createDestChunksUsingInsert();
			createShardTagsUsingInsert();
		}

		if (compareAndMove) {
			if (nonPrivilegedMode) {
				compareAndMoveChunks(true);
			} else {
				compareAndMovePrivileged();
			}
		}
		
		

		if (! skipFlushRouterConfig) {
			destShardClient.flushRouterConfig();
		}
	}

	private void stopBalancers() {

		logger.debug("stopBalancers started");
		if (sourceClusterPattern == null) {
			try {
				sourceShardClient.stopBalancer();
			} catch (MongoCommandException mce) {
				logger.error("Could not stop balancer on source shard: " + mce.getMessage());
			}
		} else {
			logger.debug("Skipping source balancer stop, patterned uri");
		}
		
		if (destClusterPattern == null) {
			try {
				destShardClient.stopBalancer();
			} catch (MongoCommandException mce) {
				logger.error("Could not stop balancer on dest shard: " + mce.getMessage());
			}
		} else {
			logger.debug("Skipping dest balancer stop, patterned uri");
		}

		logger.debug("stopBalancers complete");
	}

	private void checkAutosplit() {
		sourceShardClient.checkAutosplit();
	}
	
	public void disableSourceAutosplit() {
		sourceShardClient.disableAutosplit();
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Document getChunkQuery() {
		Document chunkQuery = new Document();
		if (includeNamespaces.size() > 0 || includeDatabases.size() > 0) {
			List inList = new ArrayList();
			List orList = new ArrayList();
			// Document orDoc = new Document("$or", orList);
			chunkQuery.append("$or", orList);
			Document inDoc = new Document("ns", new Document("$in", inList));
			orList.add(inDoc);
			// orDoc.append("ns", inDoc);
			for (Namespace includeNs : includeNamespaces) {
				inList.add(includeNs.getNamespace());
			}
			for (String dbName : includeDatabases) {
				orList.add(regex("ns", "^" + dbName + "\\."));
			}
		}
		return chunkQuery;
	}
	
	/**
	 * Create chunks on the dest side using the "split" runCommand NOTE that this
	 * will be very slow b/c of the locking process that happens with each chunk
	 */
	private void createDestChunksUsingSplitCommand() {
		createDestChunksUsingSplitCommand(null);
	}
	
	private void createDestChunksUsingSplitCommand(String nsFilter) {
		if (nsFilter == null) {
			logger.debug("createDestChunksUsingSplitCommand started");
		}
		
		MongoCollection<RawBsonDocument> sourceChunksColl = sourceShardClient.getChunksCollectionRaw();

		Document chunkQuery = getChunkQuery();
		if (nsFilter != null) {
			chunkQuery.append("ns", nsFilter);
		}
		
		List<RawBsonDocument> sourceChunks = new ArrayList<>();
		sourceChunksColl.find().sort(Sorts.ascending("ns", "min")).into(sourceChunks);
		

		String lastNs = null;
		int currentCount = 0;

		for (RawBsonDocument chunk : sourceChunks) {

			String ns = chunk.getString("ns").getValue();
			if (filterCheck(ns)) {
				continue;
			}
			
			if (!ns.equals(lastNs) && lastNs != null) {
				logger.debug(String.format("%s - created %s chunks", lastNs, ++currentCount));
				currentCount = 0;
			}
			
			destShardClient.createChunk(chunk, true, true);
			lastNs = ns;
			currentCount++;
			
		}
		if (nsFilter == null) {
			logger.debug("createDestChunksUsingSplitCommand complete");
		}
	}
	
	// TODO incomplete
	private void createMergedChunks() {
		logger.debug("createMergedChunks started");
		
		MongoCollection<RawBsonDocument> sourceChunksColl = sourceShardClient.getChunksCollectionRaw();

		Document chunkQuery = getChunkQuery();
		
		FindIterable<RawBsonDocument> sourceChunks = sourceChunksColl.find(chunkQuery).noCursorTimeout(true)
				.sort(Sorts.ascending("ns", "min"));

		String lastNs = null;
		int currentCount = 0;

		for (Iterator<RawBsonDocument> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext();) {

			RawBsonDocument chunk = sourceChunksIterator.next();
			String ns = chunk.getString("ns").getValue();
			if (filterCheck(ns)) {
				continue;
			}
			
			if (!ns.equals(lastNs) && lastNs != null) {
				logger.debug(String.format("%s - created %s chunks", lastNs, ++currentCount));
				currentCount = 0;
			}
			
			destShardClient.createChunk(chunk, true, true);
			lastNs = ns;
			currentCount++;
			
		}
		logger.debug("createDestChunksUsingSplitCommand complete");
	}

	private String getAltMapping(String sourceShardName) {
		if (! altSourceToDestShardMap.isEmpty()) {
			String newKey = altSourceToDestShardMap.get(sourceShardName);
			return newKey;
			
		} else {
			return sourceToDestShardMap.get(sourceShardName);
		}
	}
	
	private String getSourceToDestShardMapping(String sourceShardName) {
		if (! altSourceToDestShardMap.isEmpty()) {
			String newKey = altSourceToDestShardMap.get(sourceShardName);
			String result = destToSourceShardMap.get(newKey);
			return result;
			
		} else {
			return sourceToDestShardMap.get(sourceShardName);
		}
	}

	@SuppressWarnings("unchecked")
	private void createShardTagsUsingInsert() {
		logger.debug("createShardTagsUsingInsert started");

		MongoCollection<Document> sourceShardsColl = sourceShardClient.getShardsCollection();
		FindIterable<Document> sourceShards = sourceShardsColl.find(exists("tags.0"));
		for (Iterator<Document> it = sourceShards.iterator(); it.hasNext();) {
			Document shard = it.next();
			String sourceShardName = shard.getString("_id");
			String mappedShard = sourceToDestShardMap.get(sourceShardName);
			List<String> tags = (List<String>) shard.get("tags");

			for (String tag : tags) {
				Document command = new Document("addShardToZone", mappedShard).append("zone", tag);
				logger.debug(String.format("addShardToZone('%s', '%s')", mappedShard, tag));
				destShardClient.adminCommand(command);
			}
		}

		MongoCollection<Document> sourceTagsColl = sourceShardClient.getTagsCollection();
		FindIterable<Document> sourceTags = sourceTagsColl.find().sort(Sorts.ascending("ns", "min"));

		for (Iterator<Document> it = sourceTags.iterator(); it.hasNext();) {

			Document tag = it.next();
			logger.trace("tag: " + tag);
			String ns = tag.getString("ns");
			Namespace sourceNs = new Namespace(ns);
			if (filterCheck(sourceNs)) {
				continue;
			}

			Document command = new Document("updateZoneKeyRange", ns);
			command.append("min", tag.get("min"));
			command.append("max", tag.get("max"));
			command.append("zone", tag.get("tag"));
			destShardClient.adminCommand(command);
		}
		logger.debug("createShardTagsUsingInsert complete");
	}

	/**
	 * Alternative to createDestChunksUsingSplitCommand(). Preferred approach for
	 * simplicity and performance, but this requires special permissions in Atlas.
	 */
	private void createDestChunksUsingInsert() {
		logger.debug("createDestChunksUsingInsert started");
		MongoCollection<RawBsonDocument> sourceChunksColl = sourceShardClient.getChunksCollectionRaw();
		MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();
		ReplaceOptions replaceOptions = new ReplaceOptions().upsert(true);
		Document chunkQuery = getChunkQuery();
		
		AggregateIterable<Document> results = sourceShardClient.getChunksCollection().aggregate(Arrays.asList(
			Aggregates.match(chunkQuery),
			Aggregates.group("$ns", Accumulators.addToSet("shards", "$shard"))
        ));
		
		Map<String, Set<String>> nsToShardsMap = new HashMap<>();
        for (Document result : results) {
        	String ns = result.getString("_id");
        	List<String> shards = result.getList("shards",  String.class);
        	Set<String> mappedShards = new HashSet<>(shards.size());
        	for (String shard : shards) {
        		String mappedShard = this.getAltMapping(shard);
        		mappedShards.add(mappedShard);
        	}
        	nsToShardsMap.put(ns, mappedShards);
        }
		
		destShardClient.populateCollectionsMap();
		Map<String, Document> collectionsMap = destShardClient.getCollectionsMap();
		
		
		FindIterable<RawBsonDocument> sourceChunksIt = sourceChunksColl.find(chunkQuery)
				.sort(Sorts.ascending("ns", "min"));
		List<RawBsonDocument> sourceChunks = new ArrayList<>();
		sourceChunksIt.into(sourceChunks);

		String lastNs = null;
		int currentCount = 0;
		int ts = 1;
		
		Set<String> destShardNames = destShardClient.getShardsMap().keySet();

		for (RawBsonDocument chunk : sourceChunks) {

			String ns = chunk.getString("ns").getValue();
			
			Document collectionMeta = collectionsMap.get(ns);
			Namespace sourceNs = new Namespace(ns);
			if (filterCheck(sourceNs)) {
				continue;
			}
			
			String sourceShardName = chunk.getString("shard").getValue();
			//String mappedShard = sourceToDestShardMap.get(sourceShardName);
			String mappedShard = this.getAltMapping(sourceShardName);
			if (mappedShard == null) {
				throw new IllegalArgumentException(String.format("mappedShard is null, sourceShardName: %s, chunk: %s", 
						sourceShardName, chunk));
			}
			
			// Here we intentionally set the shard to the incorrect shard
			// we only need to do this for 1 chunk per namespace per shard.
			// This is done so that moveChunk causes the collection UUID to be created.
			// If we don't do this we would get UUID mismatches.
			Set<String> shards = nsToShardsMap.get(ns);
			if (shards.size() > 1 || !shards.contains(mappedShard)) {
				Set<String> t1 = new HashSet<>();
				t1.add(mappedShard);
				Set<String> diff = Sets.difference(shards, t1);
				if (! diff.isEmpty()) {
					String first = diff.iterator().next();
					mappedShard = first;
					shards.remove(mappedShard);
				}
			}
			
			Document newDoc = chunk.decode(codec);
			newDoc.append("shard", mappedShard);
			newDoc.append("lastmod", new BsonTimestamp(ts++, 0));
			newDoc.append("lastmodEpoch", collectionMeta.get("lastmodEpoch"));

			if (!ns.equals(lastNs) && lastNs != null) {
				logger.debug(String.format("%s - created %s chunks", lastNs, ++currentCount));
				currentCount = 0;
			}

			try {

				// hack to avoid "Invalid BSON field name _id.x" for compound shard keys
				RawBsonDocument rawDoc = new RawBsonDocument(newDoc, documentCodec);
				destShardClient.getChunksCollectionRaw().replaceOne(eq("_id", rawDoc.get("_id")), rawDoc, replaceOptions);

			} catch (MongoException mce) {
				logger.error(String.format("command error for namespace %s", ns), mce);
			}

			lastNs = ns;
			currentCount++;
		}

		logger.debug("createDestChunksUsingInsert complete");
	}

	public void compareChunks() {
		compareAndMoveChunks(false);
	}

	public void diffChunks(String dbName) {

		Map<String, Document> sourceChunkMap = new HashMap<String, Document>();
		MongoCollection<Document> sourceChunksColl = sourceShardClient.getChunksCollection();
		FindIterable<Document> sourceChunks = sourceChunksColl.find(regex("ns", "^" + dbName + "\\."))
				.sort(Sorts.ascending("ns", "min"));
		for (Document sourceChunk : sourceChunks) {
			String id = sourceChunk.getString("_id");
			sourceChunkMap.put(id, sourceChunk);
		}
		logger.debug("Done reading source chunks, count = " + sourceChunkMap.size());

		logger.debug("Reading destination chunks");
		Map<String, Document> destChunkMap = new HashMap<String, Document>();
		MongoCollection<Document> destChunksColl = destShardClient.getChunksCollection();
		FindIterable<Document> destChunks = destChunksColl.find(regex("ns", "^" + dbName + "\\."))
				.sort(Sorts.ascending("ns", "min"));

		for (Document destChunk : destChunks) {
			String id = destChunk.getString("_id");
			destChunkMap.put(id, destChunk);

			Document sourceChunk = sourceChunkMap.get(id);
			if (sourceChunk == null) {
				logger.debug("Source chunk not found: " + id);
				continue;
			}
			String sourceShard = sourceChunk.getString("shard");
			String mappedShard = sourceToDestShardMap.get(sourceShard);
			if (mappedShard == null) {
				throw new IllegalArgumentException(
						"No destination shard mapping found for source shard: " + sourceShard);
			}

			String destShard = destChunk.getString("shard");
			if (!destShard.equals(mappedShard)) {
				logger.warn("Chunk on wrong shard: " + id);
			}

		}
		logger.debug("Done reading destination chunks, count = " + destChunkMap.size());

	}
	
	private static String getHashIdFromChunk(RawBsonDocument sourceChunk) {
		RawBsonDocument sourceMin = (RawBsonDocument) sourceChunk.get("min");
		//ByteBuffer byteBuffer = sourceMin.getByteBuffer().asNIO();
        //byte[] minBytes = new byte[byteBuffer.remaining()];
        
		String minHash = sourceMin.toJson();
		
		RawBsonDocument sourceMax = (RawBsonDocument) sourceChunk.get("max");
		//byteBuffer = sourceMax.getByteBuffer().asNIO();
		//byte[] maxBytes = new byte[byteBuffer.remaining()];
		String maxHash = sourceMax.toJson();
		
		String ns = sourceChunk.getString("ns").getValue();
		//logger.debug(String.format("hash: %s_%s => %s_%s", sourceMin.toString(), sourceMax.toString(), minHash, maxHash));
		return String.format("%s_%s_%s", ns, minHash, maxHash);
		
	}
	
	public void compareAndMovePrivileged() {
		
		logger.debug("Starting compareAndMovePrivileged");
		
		Map<String, String> destChunkMap = readDestinationChunks();
		MongoCollection<RawBsonDocument> sourceChunksColl = sourceShardClient.getChunksCollectionRaw();
		MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();
		
		MongoCollection<RawBsonDocument> destChunksCollPriv = destShardClient.getChunksCollectionRawPrivileged();
		
		Document chunkQuery = getChunkQuery();
		AggregateIterable<Document> results = sourceShardClient.getChunksCollection().aggregate(Arrays.asList(
			Aggregates.match(chunkQuery),
			Aggregates.group("$ns", Accumulators.addToSet("shards", "$shard"))
        ));
		
		int movedCount = 0;
		int updatedCount = 0;
		int errorCount = 0;
		
		Map<String, Set<String>> nsToShardsMap = new HashMap<>();
        for (Document result : results) {
        	String ns = result.getString("_id");
        	List<String> shards = result.getList("shards",  String.class);
        	Set<String> mappedShards = new HashSet<>(shards.size());
        	for (String shard : shards) {
        		String mappedShard = this.getAltMapping(shard);
        		if (mappedShard == null) {
    				throw new IllegalArgumentException(
    						"No destination shard mapping found for source shard: " + shard);
    			}
        		mappedShards.add(mappedShard);
        		
        		FindIterable<RawBsonDocument> sourceChunks = sourceChunksColl.find(and(eq("ns", ns), eq("shard", shard)));
        		
        		boolean movedChunk = false;
        		for (RawBsonDocument sourceChunk : sourceChunks) {
        			String sourceId = getHashIdFromChunk(sourceChunk);
        			
        			String sourceNs = sourceChunk.getString("ns").getValue();
        			Namespace sourceNamespace = new Namespace(sourceNs);
        			
        			RawBsonDocument sourceMin = (RawBsonDocument) sourceChunk.get("min");
        			RawBsonDocument sourceMax = (RawBsonDocument) sourceChunk.get("max");
        			String sourceShard = sourceChunk.getString("shard").getValue();
        			
        			
        			//String sourceId = sourceChunk.getString("_id").getValue();
        			String destShard = destChunkMap.get(sourceId);

        			if (destShard == null) {
        				logger.error("Chunk with _id " + sourceId + " not found on destination");
        				errorCount++;

        			} else if (!mappedShard.equals(destShard)) {
        				
        				// We only need to moveChunk on 1 chunk per shard per ns
        				if (movedChunk) {
        					Document update = new Document("$set", new Document("shard", mappedShard));
        					Bson updateQuery = and(eq("ns", ns), eq("min", sourceMin), eq("max", sourceMax));
        					UpdateResult updateResult = destChunksCollPriv.updateOne(updateQuery, update);
        					if (updateResult.getModifiedCount() != 1) {
        						logger.error(String.format("Unexpected modifiedCount, chunk not updated ns: %s, min: %s, max: %s", ns, sourceMin, sourceMax));
        						errorCount++;
        					} else {
        						updatedCount++;
        					}
        					
        					try {
        						Number destCount = destShardClient.getCollectionCount(sourceNamespace.getDatabaseName(), sourceNamespace.getCollectionName());
        					} catch (MongoCommandException mce) {
        						String msg = mce.getErrorMessage();
        						logger.error(msg);
        					}
        					
        					
        				} else {
        					boolean moveSuccess = moveChunk(sourceNs, sourceMin, sourceMax, mappedShard);
        					if (moveSuccess) {
        						logger.debug(String.format("%s: moved chunk from %s to %s", sourceNs, destShard, mappedShard));
                				movedChunk = true;
                				movedCount++;
        					} else {
        						errorCount++;
        					}
            				
        				}
        			}
        			
        		}
        		
        	}
        	nsToShardsMap.put(ns, mappedShards);
        }
        logger.debug(String.format("Finished compareAndMovePrivileged movedCount: %s, updatedCount: %s, errorCount: %s", movedCount, updatedCount, errorCount));
		
	}
	
	private Map<String, String> readDestinationChunks() {
		logger.debug("Reading destination chunks");
		Map<String, String> destChunkMap = new HashMap<String, String>();
		MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();
		FindIterable<RawBsonDocument> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min"));

		for (RawBsonDocument destChunk : destChunks) {
			String id = getHashIdFromChunk(destChunk);
			//logger.debug("dest id: " + id);
			String shard = destChunk.getString("shard").getValue();
			destChunkMap.put(id, shard);
		}
		logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
		return destChunkMap;
	}

	public void compareAndMoveChunks(boolean doMove) {

		Map<String, String> destChunkMap = readDestinationChunks();

		MongoCollection<RawBsonDocument> sourceChunksColl = sourceShardClient.getChunksCollectionRaw();
		List<RawBsonDocument> sourceChunks = new ArrayList<>();
		sourceChunksColl.find().sort(Sorts.ascending("ns", "min")).into(sourceChunks);

		String lastNs = null;
		int currentCount = 0;
		int movedCount = 0;
		int mismatchedCount = 0;
		int matchedCount = 0;
		int missingCount = 0;
		int sourceTotalCount = 0;
		int errorCount = 0;

		for (RawBsonDocument sourceChunk : sourceChunks) {
			sourceTotalCount++;
			String sourceId = getHashIdFromChunk(sourceChunk);
			//logger.debug("source id: " + sourceId);
			
			String sourceNs = sourceChunk.getString("ns").getValue();
			Namespace sourceNamespace = new Namespace(sourceNs);
			if (filterCheck(sourceNamespace)) {
				continue;
			}

			if (!sourceNs.equals(lastNs)) {
				if (currentCount > 0) {
					logger.debug(String.format("compareAndMoveChunks - %s - complete, compared %s chunks", lastNs,
							currentCount));
					currentCount = 0;
				}
				logger.debug(String.format("compareAndMoveChunks - %s - starting", sourceNs));
			} else if (currentCount > 0 && currentCount % 10000 == 0) {
				logger.debug(
						String.format("compareAndMoveChunks - %s - currentCount: %s chunks", sourceNs, currentCount));
			}

			RawBsonDocument sourceMin = (RawBsonDocument) sourceChunk.get("min");
			RawBsonDocument sourceMax = (RawBsonDocument) sourceChunk.get("max");
			String sourceShard = sourceChunk.getString("shard").getValue();
			String mappedShard = getAltMapping(sourceShard);
			//String mappedShard = sourceToDestShardMap.get(sourceShard);
			if (mappedShard == null) {
				throw new IllegalArgumentException(
						"No destination shard mapping found for source shard: " + sourceShard);
			}
			
			//String sourceId = sourceChunk.getString("_id").getValue();
			String destShard = destChunkMap.get(sourceId);

			if (destShard == null) {
				logger.error("Chunk with _id " + sourceId + " not found on destination");
				missingCount++;

			} else if (doMove && !mappedShard.equals(destShard)) {
				//logger.debug(String.format("%s: moving chunk from %s to %s", sourceNs, destShard, mappedShard));
				if (doMove) {
					boolean moveSuccess = moveChunk(sourceNs, sourceMin, sourceMax, mappedShard);
					if (! moveSuccess) {
						errorCount++;
					}
				}

				movedCount++;

			} else if (!doMove) {
				if (!mappedShard.equals(destShard)) {
					logger.debug(String.format("mismatch: %s ==> %s", destShard, mappedShard));
					logger.debug("dest chunk is on wrong shard for sourceChunk: " + sourceChunk);
					mismatchedCount++;
				}
				matchedCount++;
			}

			currentCount++;
			lastNs = sourceNs;
		}
		logger.debug(String.format("compareAndMoveChunks - %s - complete, compared %s chunks", lastNs, currentCount));

		if (doMove) {
			logger.debug(String.format("compareAndMoveChunks complete, sourceCount: %s, destCount: %s",
					sourceTotalCount, destChunkMap.size()));
		} else {
			logger.debug(String.format(
					"compareAndMoveChunks complete, sourceCount: %s, destCount: %s, mismatchedCount: %s, missingCount: %s",
					sourceTotalCount, destChunkMap.size(), mismatchedCount, missingCount));
		}

	}

	@SuppressWarnings("unchecked")
	public void compareShardCounts() {

		logger.debug("Starting compareShardCounts mode");

		Document listDatabases = new Document("listDatabases", 1);
		Document sourceDatabases = sourceShardClient.adminCommand(listDatabases);
		Document destDatabases = destShardClient.adminCommand(listDatabases);

		List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
		List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

		populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
		populateDbMap(destDatabaseInfo, destDbInfoMap);

		for (Document sourceInfo : sourceDatabaseInfo) {
			String dbName = sourceInfo.getString("name");

			if (filtered && !includeDatabasesAll.contains(dbName) || dbName.equals("config")) {
				logger.debug("Ignore " + dbName + " for compare, filtered");
				continue;
			}

			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {
				logger.debug(String.format("Found matching database %s", dbName));
				
				long sourceTotal = 0;
				long destTotal = 0;
				int collCount = 0;

				MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(dbName);
				MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.startsWith("system.")) {
						continue;
					}
					
					Namespace ns = new Namespace(dbName, collectionName);
					if (filtered && !includeNamespaces.contains(ns)) {
						//logger.debug("include: " + includeNamespaces);
						continue;
					}
					

					long[] result = doCounts(sourceDb, destDb, collectionName);
					sourceTotal += result[0];
					destTotal += result[1];
					collCount++;
				}
				logger.debug("Database {} - source count sourceTotal: {}, dest count sourceTotal {}", dbName, sourceTotal, destTotal);
			} else {
				logger.warn(String.format("Destination db not found, name: %s", dbName));
			}
		}
	}
	
	private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
		return doCounts(sourceDb, destDb, collectionName, null);
	}

	private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName, Bson query) {

		long[] result = new long[2];
		Long sourceCount = null;
		Long destCount = null;
		if (query == null) {
			sourceCount = sourceDb.getCollection(collectionName).countDocuments();
			destCount = destDb.getCollection(collectionName).countDocuments();
		} else {
			//db.getCollection(collectionName).countDocuments();
			sourceCount = sourceDb.getCollection(collectionName).countDocuments(query);
			destCount = destDb.getCollection(collectionName).countDocuments(query);
		}
		
		result[0] = sourceCount;
		result[1] = destCount;
		
		if (sourceCount.equals(destCount)) {
			logger.debug(String.format("%s.%s count matches: %s", sourceDb.getName(), collectionName, sourceCount));
			return result;
		} else {
			logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s, query: %s", sourceDb.getName(), collectionName,
					sourceCount, destCount, query));
			return result;
		}
	}
	
	public void compareChunkCounts() {
		for (String databaseName : sourceShardClient.listDatabaseNames()) {
			MongoDatabase db = sourceShardClient.getMongoClient().getDatabase(databaseName);

			if (databaseName.equals("admin") || databaseName.equals("config")
					|| databaseName.contentEquals("local")) {
				continue;
			}

			for (Document collectionInfo : db.listCollections()) {
				String collectionName = (String) collectionInfo.get("name");
				if (collectionName.endsWith(".create")) {
					continue;
				}
				
				Namespace ns = new Namespace(databaseName, collectionName);
				if (filtered && !includeNamespaces.contains(ns)) {
					logger.debug("compareChunkCounts skipping {}, filtered", ns);
					continue;
				}
				compareChunkCounts(ns);
			}
		}
	}

	// TODO - this is incomplete
	public void compareChunkCounts(Namespace ns) {
		destShardClient.populateCollectionsMap();
		Document shardCollection = destShardClient.getCollectionsMap().get(ns.getNamespace());
		if (shardCollection == null) {
			logger.warn("Collection {} is not sharded, cannot do chunk compare", ns);
		} else {
			MongoDatabase sourceDb = sourceShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
			MongoDatabase destDb = destShardClient.getMongoClient().getDatabase(ns.getDatabaseName());
			
			Document shardKeysDoc = (Document) shardCollection.get("key");
			Set<String> shardKeys = shardKeysDoc.keySet();
		
			// use dest chunks as reference, may be smaller
			MongoCollection<Document> chunksCollection = destShardClient.getChunksCollection();
			// int chunkCount = (int)sourceChunksColl.countDocuments(eq("ns",
			// ns.getNamespace()));
	
			FindIterable<Document> sourceChunks = chunksCollection.find(eq("ns", ns.getNamespace()))
					.sort(Sorts.ascending("min"));
			for (Document sourceChunk : sourceChunks) {
				String id = sourceChunk.getString("_id");
				// each chunk is inclusive of min and exclusive of max
				Document min = (Document) sourceChunk.get("min");
				Document max = (Document) sourceChunk.get("max");
				Bson chunkQuery = null;
	
				if (shardKeys.size() > 1) {
					List<Bson> filters = new ArrayList<Bson>(shardKeys.size());
					for (String key : shardKeys) {
						filters.add(and(gte(key, min.get(key)), lt(key, max.get(key))));
					}
					chunkQuery = and(filters);
				} else {
					String key = shardKeys.iterator().next();
					chunkQuery = and(gte(key, min.get(key)), lt(key, max.get(key)));
				}
				
				long[] result = doCounts(sourceDb, destDb, ns.getCollectionName(), chunkQuery);
			}
		}
	}

	public void compareCollectionUuids() {
		destShardClient.compareCollectionUuids();
	}

	private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
		for (Document dbInfo : dbInfoList) {
			databaseMap.put(dbInfo.getString("name"), dbInfo);
		}
	}

	private boolean moveChunk(String namespace, RawBsonDocument min, RawBsonDocument max, String moveToShard) {
		Document moveChunkCmd = new Document("moveChunk", namespace);
		moveChunkCmd.append("bounds", Arrays.asList(min, max));
		moveChunkCmd.append("to", moveToShard);
		try {
			destShardClient.adminCommand(moveChunkCmd);
		} catch (MongoCommandException mce) {
			logger.warn(String.format("moveChunk error ns: %s, message: %s", namespace, mce.getMessage()));
			return false;
		}
		return true;
	}

	public void shardDestinationCollections() {
		// Don't use the insert method regardless, because that can cause us to
		// miss UUIDs for MongoDB 3.6+
		shardDestinationCollectionsUsingShardCommand();
	}

	private void shardDestinationCollectionsUsingInsert() {
		logger.debug("shardDestinationCollectionsUsingInsert(), privileged mode");

		MongoCollection<RawBsonDocument> destColls = destShardClient.getConfigDb().getCollection("collections",
				RawBsonDocument.class);
		ReplaceOptions options = new ReplaceOptions().upsert(true);

		for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

			String nsStr = (String) sourceColl.get("_id");
			Namespace ns = new Namespace(nsStr);
			if (filterCheck(ns)) {
				continue;
			}

			// hack to avoid "Invalid BSON field name _id.x" for compound shard keys
			RawBsonDocument rawDoc = new RawBsonDocument(sourceColl, documentCodec);
			destColls.replaceOne(new Document("_id", nsStr), rawDoc, options);
		}

		logger.debug("shardDestinationCollectionsUsingInsert() complete");
	}

	private void shardDestinationCollectionsUsingShardCommand() {
		logger.debug("shardDestinationCollectionsUsingShardCommand(), non-privileged mode");

		for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

			String nsStr = (String) sourceColl.get("_id");
			Namespace ns = new Namespace(nsStr);

			if (filterCheck(ns)) {
				continue;
			}
			shardCollection(sourceColl);

			if ((boolean) sourceColl.get("noBalance", false)) {
				// TODO there is no disableBalancing command so this is not
				// possible in Atlas
				// destClient.getDatabase("admin").runCommand(new Document("",
				// ""));
				logger.warn(String.format("Balancing is disabled for %s, this is not possible in Atlas", nsStr));
			}
		}
		logger.debug("shardDestinationCollectionsUsingShardCommand() complete");
	}

	/**
	 * Take the sourceColl as a "template" to shard on the destination side
	 * 
	 * @param sourceColl
	 */
	private Document shardCollection(ShardCollection sourceColl) {
		Document shardCommand = new Document("shardCollection", sourceColl.getId());
		shardCommand.append("key", sourceColl.getKey());

		// apparently unique is not always correct here, there are cases where unique is
		// false
		// here but the underlying index is unique
		shardCommand.append("unique", sourceColl.isUnique());
		if (sourceColl.getDefaultCollation() != null) {
			shardCommand.append("collation", LOCALE_SIMPLE);
		}

		Document result = null;
		try {
			result = destShardClient.adminCommand(shardCommand);
		} catch (MongoCommandException mce) {
			if (mce.getCode() == 20) {
				logger.debug(String.format("Sharding already enabled for %s", sourceColl.getId()));
			} else {
				throw mce;
			}
		}
		return result;
	}

	private Document shardCollection(Document sourceColl) {
		Document shardCommand = new Document("shardCollection", sourceColl.get("_id"));

		Document key = (Document) sourceColl.get("key");
		shardCommand.append("key", key);

		// apparently unique is not always correct here, there are cases where unique is
		// false
		// here but the underlying index is unique
		shardCommand.append("unique", sourceColl.get("unique"));

		Object key1 = key.values().iterator().next();
		if ("hashed".equals(key1)) {
			shardCommand.append("numInitialChunks", 1);
		}

		// TODO fixme!!!
//        if (sourceColl.getDefaultCollation() != null) {
//            shardCommand.append("collation", LOCALE_SIMPLE);
//        }

		Document result = null;
		try {
			result = destShardClient.adminCommand(shardCommand);
		} catch (MongoCommandException mce) {
			if (mce.getCode() == 20) {
				logger.debug(String.format("Sharding already enabled for %s", sourceColl.get("_id")));
			} else {
				logger.error(String.format("Error sharding collection %s", sourceColl.get("_id")));
				//throw mce;
			}
		}
		return result;
	}

	/**
	 * 
	 * @param sync - THIS WILL shard on the dest side if not in sync
	 */
	public void diffShardedCollections(boolean sync) {
		logger.debug("diffShardedCollections()");
		sourceShardClient.populateCollectionsMap();
		destShardClient.populateCollectionsMap();

		for (Document sourceColl : sourceShardClient.getCollectionsMap().values()) {

			String nsStr = (String) sourceColl.get("_id");
			Namespace ns = new Namespace(nsStr);
			if (filterCheck(ns)) {
				continue;
			}

			Document destCollection = destShardClient.getCollectionsMap().get(sourceColl.get("_id"));

			if (destCollection == null) {
				logger.debug("Destination collection not found: " + sourceColl.get("_id") + " sourceKey:"
						+ sourceColl.get("key"));
				if (sync) {
					try {
						Document result = shardCollection(sourceColl);
						logger.debug("Sharded: " + result);
					} catch (MongoCommandException mce) {
						logger.error("Error sharding", mce);
					}
				}
			} else {
				if (sourceColl.get("key").equals(destCollection.get("key"))) {
					logger.debug("Shard key match for " + sourceColl);
				} else {
					logger.warn("Shard key MISMATCH for " + sourceColl + " sourceKey:" + sourceColl.get("key")
							+ " destKey:" + destCollection.get("key"));
				}
			}
		}
	}

	public void enableDestinationSharding() {
		sourceShardClient.populateShardMongoClients();
		
		logger.debug("enableDestinationSharding()");
		MongoCollection<Document> databasesColl = sourceShardClient.getConfigDb().getCollection("databases");

		// todo, what about unsharded collections, don't we need to movePrimary for
		// them?
		// FindIterable<Document> databases = databasesColl.find(eq("partitioned",
		// true));
		FindIterable<Document> databases = databasesColl.find();

		List<Document> databasesList = new ArrayList<Document>();
		databases.into(databasesList);
		for (Document database : databasesList) {
			String databaseName = database.getString("_id");
			if (databaseName.equals("admin") || databaseName.equals("system") || databaseName.equals("local") 
					|| databaseName.contains("$")) {
				continue;
			}
			String primary = database.getString("primary");
			String xx = sourceToDestShardMap.get(primary);
			String mappedPrimary = getAltMapping(primary);
			logger.debug("database: " + databaseName + ", primary: " + primary + ", mappedPrimary: " + mappedPrimary);
            if (mappedPrimary == null) {
                logger.warn("Shard mapping not found for shard " + primary);
            }

			if (filtered && !includeDatabasesAll.contains(databaseName)) {
				logger.trace("Database " + databaseName + " filtered, not sharding on destination");
				continue;
			}

			Document dest = destShardClient.getConfigDb().getCollection("databases")
					.find(new Document("_id", databaseName)).first();
			if (database.getBoolean("partitioned", true)) {
				logger.debug(String.format("enableSharding: %s", databaseName));
				try {
					destShardClient.adminCommand(new Document("enableSharding", databaseName));
				} catch (MongoCommandException mce) {
					if (mce.getCode() == 23 && mce.getErrorMessage().contains("sharding already enabled")) {
						logger.debug("Sharding already enabled: " + databaseName);
					} else {
						throw mce;
					}
				}

			}

			// this needs to be the atlas-xxx id
			String zz = destToSourceShardMap.get(mappedPrimary);
			MongoClient primaryClient = sourceShardClient.getShardMongoClient(zz);
			List<String> primaryDatabasesList = new ArrayList<String>();
			primaryClient.listDatabaseNames().into(primaryDatabasesList);
			if (!primaryDatabasesList.contains(databaseName)) {
				logger.debug("Database: " + databaseName + " does not exist on source shard, skipping");
				continue;
			}

			//dest = destShardClient.getDatabasesCollection().find(new Document("_id", databaseName)).first();
			
			//if (dest == null) {
			dest = destShardClient.createDatabase(databaseName);
			logger.debug("dest db: " + dest);

			String destPrimary = dest.getString("primary");
			if (mappedPrimary.equals(destPrimary)) {
				logger.debug("Primary shard already matches for database: " + databaseName);
			} else {
				logger.debug(
						"movePrimary for database: " + databaseName + " from " + destPrimary + " to " + mappedPrimary);
				try {
					destShardClient.adminCommand(new Document("movePrimary", databaseName).append("to", mappedPrimary));
				} catch (MongoCommandException mce) {
					// TODO check if exists on source rather than this
					logger.warn("movePrimary for database: " + databaseName + " failed. Maybe it doesn't exist?");
				}
			}

		}
		logger.debug("enableDestinationSharding() complete");
	}

	/**
	 * Drop based on config.databases
	 */
	public void dropDestinationDatabases() {
		logger.debug("dropDestinationDatabases()");
		destShardClient.populateShardMongoClients();
		MongoCollection<Document> databasesColl = sourceShardClient.getDatabasesCollection();
		FindIterable<Document> databases = databasesColl.find();
		List<String> databasesList = new ArrayList<String>();

		for (Document database : databases) {
			String databaseName = database.getString("_id");

			if (filtered && !includeDatabases.contains(databaseName)) {
				logger.trace("Database " + databaseName + " filtered, not dropping on destination");
				continue;
			} else {
				databasesList.add(databaseName);
			}
		}
		destShardClient.dropDatabases(databasesList);
		logger.debug("dropDestinationDatabases() complete");
	}

	public void dropDestinationDatabasesAndConfigMetadata() {
		logger.debug("dropDestinationDatabasesAndConfigMetadata()");
		destShardClient.populateShardMongoClients();
		MongoCollection<Document> databasesColl = sourceShardClient.getDatabasesCollection();
		FindIterable<Document> databases = databasesColl.find();
		List<String> databasesList = new ArrayList<String>();

		for (Document database : databases) {
			String databaseName = database.getString("_id");

			if (filtered && !includeDatabases.contains(databaseName)) {
				logger.trace("Database " + databaseName + " filtered, not dropping on destination");
				continue;
			} else {
				databasesList.add(databaseName);
			}
		}
		destShardClient.dropDatabasesAndConfigMetadata(databasesList);
		logger.debug("dropDestinationDatabasesAndConfigMetadata() complete");

	}

	public void cleanupOrphans() {
		logger.debug("cleanupOrphans()");
		sourceShardClient.populateCollectionsMap();
		sourceShardClient.populateShardMongoClients();
		CleanupOrphaned cleaner = new CleanupOrphaned(sourceShardClient, includeNamespaces);
		cleaner.cleanupOrphans(cleanupOrphansSleepMillis);
	}

	public void cleanupOrphansDest() {
		logger.debug("cleanupOrphansDest()");
		destShardClient.populateCollectionsMap();
		destShardClient.populateShardMongoClients();
		CleanupOrphaned cleaner = new CleanupOrphaned(destShardClient, includeNamespaces);
		cleaner.cleanupOrphans(cleanupOrphansSleepMillis);
	}

	public String getSourceClusterUri() {
		return sourceClusterUri;
	}

	public void setSourceClusterUri(String sourceClusterUri) {
		this.sourceClusterUri = sourceClusterUri;
	}

	public String getDestClusterUri() {
		return destClusterUri;
	}

	public void setDestClusterUri(String destClusterUri) {
		this.destClusterUri = destClusterUri;
	}

	public boolean isDropDestDbs() {
		return dropDestDbs;
	}

	public void setDropDestDbs(boolean dropDestinationCollectionsIfExisting) {
		this.dropDestDbs = dropDestinationCollectionsIfExisting;
	}

	public void setDoChunkCounts(boolean doChunkCounts) {
		this.doChunkCounts = doChunkCounts;
	}

	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
				includeDatabasesAll.add(ns.getDatabaseName());
			} else {
				includeDatabases.add(nsStr);
				includeDatabasesAll.add(nsStr);
			}
		}
	}

	public void setShardMappings(String[] shardMap) {
		this.shardMap = shardMap;
	}

	public void shardToRs() throws ExecuteException, IOException {

		logger.debug("shardToRs() starting");

		for (Shard source : sourceShardClient.getShardsMap().values()) {
			logger.debug("sourceShard: " + source.getId());
			MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());

			// Source setup
			mongomirror.setSourceHost(source.getHost());

			MongoCredential sourceCredentials = sourceShardClient.getConnectionString().getCredential();
			if (sourceCredentials != null) {
				mongomirror.setSourceUsername(sourceCredentials.getUserName());
				mongomirror.setSourcePassword(new String(sourceCredentials.getPassword()));
				mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
			}
			if (sourceShardClient.getConnectionString().getSslEnabled() != null) {
				mongomirror.setSourceSsl(sourceShardClient.getConnectionString().getSslEnabled());
			}

			ClusterDescription cd = destShardClient.getMongoClient().getClusterDescription();
			ServerDescription s1 = cd.getServerDescriptions().get(0);
			String setName = s1.getSetName();
			

			// destMongoClientURI.getCredentials().getSource();
			ConnectionString cs = destShardClient.getConnectionString();
			String host = destShardClient.getConnectionString().getHosts().get(0); // TODO verify

			mongomirror.setDestinationHost(setName + "/" + host);
			MongoCredential destCredentials = destShardClient.getConnectionString().getCredential();
			if (destCredentials != null) {
				mongomirror.setDestinationUsername(destCredentials.getUserName());
				mongomirror.setDestinationPassword(new String(destCredentials.getPassword()));
				mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
			}

			if (destShardClient.getConnectionString().getSslEnabled() == null
					|| destShardClient.getConnectionString().getSslEnabled().equals(Boolean.FALSE)) {
				// TODO - this is only in "hacked" mongomirror
				mongomirror.setDestinationNoSSL(true);
			}

			for (Namespace ns : includeNamespaces) {
				mongomirror.addIncludeNamespace(ns);
			}

			for (String dbName : includeDatabases) {
				mongomirror.addIncludeDatabase(dbName);
			}

//            if (dropDestinationCollectionsIfExisting) {
//                if (! destShard.isMongomirrorDropped()) {
//                    // for n:m shard mapping, only set drop on the first mongomiirror that we start,
//                    // since there will be multiple mongomirrors pointing to the same destination
//                    // and we would drop data that had started to copy
//                    mongomirror.setDrop(dropDestinationCollectionsIfExisting);
//                    destShard.setMongomirrorDropped(true);
//                }
//            }

			mongomirror.setMongomirrorBinary(mongomirrorBinary);

			String dateStr = formatter.format(LocalDateTime.now());
			
			// TODO
			//mongomirror.setBookmarkFile(String.format("%s_%s.timestamp", source.getId(), dateStr));
			mongomirror.setBookmarkFile(source.getId() + ".timestamp");

			mongomirror.setNumParallelCollections(numParallelCollections);
			mongomirror.execute(dryRun);
			try {
				Thread.sleep(sleepMillis);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
	
	public void mongomirrorTailFromLatestOplogTs(String startingTs)  throws IOException {
		logger.debug("Starting mongomirrorTailFromTs, startingTs: {}", startingTs);
		sourceShardClient.populateShardMongoClients();
		Collection<Shard> shards = sourceShardClient.getShardsMap().values();
		for (Shard shard : shards) {
			ShardTimestamp st = sourceShardClient.populateLatestOplogTimestamp(shard.getId(), startingTs);
			logger.debug(st.toString());
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(shard.getId() + ".timestamp")));
				writer.write(shard.getRsName());
				writer.newLine();
				writer.write(String.valueOf(st.getTimestamp().getValue()));
				writer.close();
				
			} catch (IOException e) {
				logger.error(String.format("Error writing timestamp file for shard %s", shard.getId()), e);
				throw e;
			}
		}
		mongomirror();
	}
	
	public void mongomirrorTailFromTs(String ts) throws IOException {
		String[] tsParts = ts.split(",");
		int seconds = Integer.parseInt(tsParts[0]);
		int increment = Integer.parseInt(tsParts[1]);
		BsonTimestamp bsonTs = new BsonTimestamp(seconds, increment);
		mongomirrorTailFromTs(bsonTs);
	}
	
	public void mongomirrorTailFromNow() throws IOException {
		
		
		long now = System.currentTimeMillis();
		long nowSeconds = now/1000l;
		BsonTimestamp nowBson = new BsonTimestamp((int)nowSeconds, 1);
		logger.debug(String.format("Starting mongomirrorTailFromTs, now: %s, nowSeconds: %s, nowBson: %s", 
				now, nowSeconds, nowBson));
		mongomirrorTailFromTs(nowBson);
	}
	
	private void mongomirrorTailFromTs(BsonTimestamp nowBson) throws IOException {
		
		
		//sourceShardClient.populateShardMongoClients();
		Collection<Shard> shards = sourceShardClient.getShardsMap().values();
		logger.debug("shardCount: " + shards.size());
		
		for (Shard shard : shards) {
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(new File(shard.getId() + ".timestamp")));
				writer.write(shard.getRsName());
				writer.newLine();
				writer.write(String.valueOf(nowBson.getValue()));
				writer.close();
				
			} catch (IOException e) {
				logger.error(String.format("Error writing timestamp file for shard %s", shard.getId()), e);
				throw e;
			}
		}
		mongomirror();
	}

	public void mongomirror() throws ExecuteException, IOException {

		destShardClient.populateShardMongoClients();

		List<MongoMirrorRunner> mongomirrors = new ArrayList<>(sourceShardClient.getShardsMap().size());
		
		int httpStatusPort = mongoMirrorStartPort;

		for (Shard source : sourceShardClient.getShardsMap().values()) {
			
			MongoMirrorRunner mongomirror = new MongoMirrorRunner(source.getId());
			mongomirrors.add(mongomirror);

			mongomirror.setSourceHost(source.getHost());

			MongoCredential sourceCredentials = sourceShardClient.getConnectionString().getCredential();
			if (sourceCredentials != null) {
				mongomirror.setSourceUsername(sourceCredentials.getUserName());
				mongomirror.setSourcePassword(new String(sourceCredentials.getPassword()));
				mongomirror.setSourceAuthenticationDatabase(sourceCredentials.getSource());
			}
			if (sourceShardClient.getConnectionString().getSslEnabled() != null) {
				mongomirror.setSourceSsl(sourceShardClient.getConnectionString().getSslEnabled());
			}

			// Destination setup
			ClusterDescription cd = destShardClient.getMongoClient().getClusterDescription();

			// destMongoClientURI.getCredentials().getSource();
			String destShardId = sourceToDestShardMap.get(source.getId());
			Shard dest = destShardClient.getShardsMap().get(destShardId);
			String host = dest.getHost();
			
			logger.debug(String.format("Creating MongoMirrorRunner for %s ==> %s", source.getId(), dest.getId()));

			mongomirror.setDestinationHost(host);

			MongoCredential destCredentials = destShardClient.getConnectionString().getCredential();
			if (destCredentials != null) {
				mongomirror.setDestinationUsername(destCredentials.getUserName());
				mongomirror.setDestinationPassword(new String(destCredentials.getPassword()));
				mongomirror.setDestinationAuthenticationDatabase(destCredentials.getSource());
			}

			if (destShardClient.getConnectionString().getSslEnabled() == null
					|| destShardClient.getConnectionString().getSslEnabled().equals(Boolean.FALSE)) {
				// TODO - this is only in "hacked" mongomirror
				mongomirror.setDestinationNoSSL(true);
			}

			for (Namespace ns : includeNamespaces) {
				mongomirror.addIncludeNamespace(ns);
			}

			for (String dbName : includeDatabases) {
				mongomirror.addIncludeDatabase(dbName);
			}

			mongomirror.setMongomirrorBinary(mongomirrorBinary);
			
//			String dateStr = null;
//			if (bookmarkFilePrefix != null) {
//				dateStr = bookmarkFilePrefix;
//			} else {
//				dateStr = formatter.format(LocalDateTime.now());
//			}
//			mongomirror.setBookmarkFile(String.format("%s_%s.timestamp", dateStr, source.getId()));
			mongomirror.setBookmarkFile(source.getId() + ".timestamp");

			mongomirror.setNumParallelCollections(numParallelCollections);
			mongomirror.setWriteConcern(writeConcern);
			mongomirror.setHttpStatusPort(httpStatusPort++);

			logger.debug("here: skipBuildIndexes=" + skipBuildIndexes);
			if (skipBuildIndexes) {
				mongomirror.setSkipBuildIndexes(skipBuildIndexes);
			}
			if (compressors != null) {
				mongomirror.setCompressors(compressors);
			}
			if (oplogBasePath != null) {
				mongomirror.setOplogPath(String.format("%s/%s", oplogBasePath, source.getId()));
			}
			if (collStatsThreshold != null) {
				mongomirror.setCollStatsThreshold(collStatsThreshold);
			}
			
			mongomirror.execute(dryRun);
			
			try {
				Thread.sleep(sleepMillis);
			} catch (InterruptedException e) {
			}
		}
		
		if (dryRun) {
			return;
		}

		while (true) {
			try {
				Thread.sleep(5 * 1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (MongoMirrorRunner mongomirror : mongomirrors) {
				MongoMirrorStatus status = mongomirror.checkStatus();
				if (status == null) {
					continue;
				}
				if (status.getErrorMessage() != null) {
					logger.error(
							String.format("%s - mongomirror error %s", mongomirror.getId(), status.getErrorMessage()));
				} else if (status.isInitialSync()) {
					MongoMirrorStatusInitialSync st = (MongoMirrorStatusInitialSync) status;
					if (st.isCopyingIndexes()) {
						logger.debug(String.format("%-15s - %-18s %-22s", mongomirror.getId(), status.getStage(),
								status.getPhase()));
					} else {
						double cs = st.getCompletionPercent();
						logger.debug(String.format("%-15s - %-18s %-22s %6.2f%% complete", mongomirror.getId(),
								status.getStage(), status.getPhase(), cs));
					}

				} else if (status.isOplogSync()) {
					MongoMirrorStatusOplogSync st = (MongoMirrorStatusOplogSync) status;
					logger.debug(String.format("%-15s - %-18s %-22s %s lag from source", mongomirror.getId(),
							status.getStage(), status.getPhase(), st.getLagPretty()));
				} else {
					logger.debug(String.format("%-15s - %-18s %-22s", mongomirror.getId(), status.getStage(),
							status.getPhase()));
				}

			}
		}

	}

	public void setMongomirrorBinary(String binaryPath) {
		if (binaryPath != null) {
			this.mongomirrorBinary = new File(binaryPath);
		}
	}

	public void setSleepMillis(String optionValue) {
		if (optionValue != null) {
			this.sleepMillis = Long.parseLong(optionValue);
		}
	}

	public void setNumParallelCollections(String numParallelCollections) {
		this.numParallelCollections = numParallelCollections;
	}

	public void setNonPrivilegedMode(boolean nonPrivilegedMode) {
		this.nonPrivilegedMode = nonPrivilegedMode;
	}

	public void flushRouterConfig() {
		destShardClient.flushRouterConfig();
	}

	public void setDropDestDbsAndConfigMetadata(boolean dropDestinationConfigMetadata) {
		this.dropDestDbsAndConfigMetadata = dropDestinationConfigMetadata;
	}

	public void setSslAllowInvalidHostnames(boolean sslAllowInvalidHostnames) {
		this.sslAllowInvalidHostnames = sslAllowInvalidHostnames;
	}

	public void setSslAllowInvalidCertificates(boolean sslAllowInvalidCertificates) {
		this.sslAllowInvalidCertificates = sslAllowInvalidCertificates;
	}

	public void setPreserveUUIDs(boolean preserveUUIDs) {
		this.preserveUUIDs = preserveUUIDs;
	}

	public void setCompressors(String compressors) {
		this.compressors = compressors;
	}

	public void setWriteConcern(String writeConcern) {
		this.writeConcern = writeConcern;
	}

	public void setCleanupOrphansSleepMillis(String sleepMillisString) {
		if (sleepMillisString != null) {
			this.cleanupOrphansSleepMillis = Long.parseLong(sleepMillisString);
		}
	}
	
	public void setMongoMirrorStartPort(int mongoMirrorStartPort) {
		this.mongoMirrorStartPort = mongoMirrorStartPort;
	}

	public void setOplogBasePath(String oplogBasePath) {
		this.oplogBasePath = oplogBasePath;
	}

	public void setBookmarkFilePrefix(String bookmarkFilePrefix) {
		this.bookmarkFilePrefix = bookmarkFilePrefix;
	}

	public void setReverseSync(boolean reverseSync) {
		this.reverseSync = reverseSync;
	}

	public void setSkipBuildIndexes(boolean skipBuildIndexes) {
		this.skipBuildIndexes = skipBuildIndexes;
	}

	public boolean isSkipFlushRouterConfig() {
		return skipFlushRouterConfig;
	}

	public void setSkipFlushRouterConfig(boolean skipFlushRouterConfig) {
		this.skipFlushRouterConfig = skipFlushRouterConfig;
	}

	public String getSourceClusterPattern() {
		return sourceClusterPattern;
	}

	public void setSourceClusterPattern(String sourceClusterPattern) {
		this.sourceClusterPattern = sourceClusterPattern;
	}

	public String getDestClusterPattern() {
		return destClusterPattern;
	}

	public void setDestClusterPattern(String destClusterPattern) {
		this.destClusterPattern = destClusterPattern;
	}

	public String getSourceRsPattern() {
		return sourceRsPattern;
	}

	public void setSourceRsPattern(String sourceRsPattern) {
		this.sourceRsPattern = sourceRsPattern;
	}

	public String getDestRsPattern() {
		return destRsPattern;
	}

	public void setDestRsPattern(String destRsPattern) {
		this.destRsPattern = destRsPattern;
	}

	public int getCollStatsThreshold() {
		return collStatsThreshold;
	}

	public void setCollStatsThreshold(int collStatsThreshold) {
		this.collStatsThreshold = collStatsThreshold;
	}

	public void setDestCsrsUri(String destCsrsUri) {
		this.destCsrsUri = destCsrsUri;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public boolean isShardToRs() {
		return shardToRs;
	}

	public void setShardToRs(boolean shardToRs) {
		this.shardToRs = shardToRs;
	}
}
