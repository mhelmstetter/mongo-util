package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.eq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.collections.MapUtils;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.client.MongoCollection;
import com.mongodb.model.Megachunk;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.ShardClient.ShardClientType;
import com.mongodb.util.bson.Base64;
import com.mongodb.util.bson.BsonUtils;
import com.mongodb.util.bson.BsonUuidUtil;

public class ChunkManager {

	private static Logger logger = LoggerFactory.getLogger(ChunkManager.class);
	
	private BsonDocument sourceChunkQuery;
	private BsonDocument destChunkQuery;
	
	private ShardClient destShardClient;
	private ShardClient sourceShardClient;
	
	private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();
	private Map<String, String> destToSourceShardMap = new HashMap<String, String>();
	
	private Map<String, String> altSourceToDestShardMap = new HashMap<String, String>();
	
	private BaseConfiguration config;
	
	public ChunkManager (BaseConfiguration config) {
		this.config = config;
	}
	
	@SuppressWarnings("unchecked")
	public void initalize() {
		
		String source = config.getSourceClusterUri() == null ? config.getSourceClusterPattern() : config.getSourceClusterUri();
		String dest = config.getDestClusterUri() == null ? config.getDestClusterPattern() : config.getDestClusterUri();
		
		ShardClientType shardClientType = ShardClientType.SHARDED_NO_SRV;
		
		if (config.getShardMap() != null) {
			// shardMap is for doing an uneven shard mapping, e.g. 10 shards on source
			// down to 5 shards on destination
			logger.debug("Custom n:m shard mapping");
			
			for (String mapping : config.getShardMap()) {
				String[] mappings = mapping.split("\\|");
				logger.debug(mappings[0] + " ==> " + mappings[1]);
				sourceToDestShardMap.put(mappings[0], mappings[1]);
			}
			
			
			sourceShardClient = new ShardClient("source", source, sourceToDestShardMap.keySet(), shardClientType);
			config.setSourceShardClient(sourceShardClient);
			destShardClient = new ShardClient("dest", dest, sourceToDestShardMap.values(), shardClientType);
			config.setDestShardClient(destShardClient);
			
			sourceShardClient.setRsSsl(config.getSourceRsSsl());
			sourceShardClient.setRsPattern(config.getSourceRsPattern());
			destShardClient.setRsPattern(config.getDestRsPattern());
			sourceShardClient.setRsStringsManual(config.getSourceRsManual());
			destShardClient.setRsStringsManual(config.getDestRsManual());
			sourceShardClient.setRsRegex(config.getSourceRsRegex());
			destShardClient.setRsRegex(config.getDestRsRegex());
			destShardClient.setCsrsUri(config.getDestCsrsUri());
			
			sourceShardClient.init();
			destShardClient.init();
			
			config.setSourceShards(sourceShardClient.getShardsMap().keySet());
			config.setDestShards(sourceShardClient.getShardsMap().keySet());
			
			
		} else {
			logger.debug("Default 1:1 shard mapping");
			
			sourceShardClient = new ShardClient("source", source, null, shardClientType);
			config.setSourceShardClient(sourceShardClient);
			destShardClient = new ShardClient("dest", dest, null, shardClientType);
			config.setDestShardClient(destShardClient);
			sourceShardClient.setRsSsl(config.getSourceRsSsl());
			sourceShardClient.setRsPattern(config.getSourceRsPattern());
			destShardClient.setRsPattern(config.getDestRsPattern());
			sourceShardClient.setRsStringsManual(config.getSourceRsManual());
			destShardClient.setRsStringsManual(config.getDestRsManual());
			sourceShardClient.setRsRegex(config.getSourceRsRegex());
			destShardClient.setRsRegex(config.getDestRsRegex());
			destShardClient.setCsrsUri(config.getDestCsrsUri());
			
			sourceShardClient.init();
			destShardClient.init();
			
			config.setSourceShards(sourceShardClient.getShardsMap().keySet());
			config.setDestShards(sourceShardClient.getShardsMap().keySet());
			
			logger.debug("Source shard count: " + sourceShardClient.getShardsMap().size());
			// default, just match up the shards 1:1
			int index = 0;
			
			Map<String, Shard> sourceTertiaryMap = sourceShardClient.getTertiaryShardsMap();
			
			//Map<String, Shard> sourceShardsMap = sourceTertiaryMap.isEmpty() ?  sourceShardClient.getShardsMap() : sourceTertiaryMap;
			Map<String, Shard> sourceShardsMap = sourceShardClient.getShardsMap();
			
			List<Shard> destList = new ArrayList<Shard>(destShardClient.getShardsMap().values());
			
			if (config.getShardMap() == null && sourceShardsMap.size() != destList.size() && !config.isShardToRs()) {
				throw new IllegalArgumentException(String.format("disparate shard counts requires shardMap to be defined, sourceShardCount: %s, destShardCount: %s", 
						sourceShardsMap.size(), destList.size()));
			}
			
			if (! config.isShardToRs()) {
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
		initializeSourceChunkQuery();
		
		// reverse map
		destToSourceShardMap = MapUtils.invertMap(sourceToDestShardMap);
		
//		if (config.getSourceClusterPattern() == null && ! sourceShardClient.isMongos()) {
//			throw new IllegalArgumentException("source connection must be to a mongos router");
//		}
	}
	
	public List<Megachunk> getMegaChunks(Map<String, RawBsonDocument> chunksCache, ShardClient shardClient) {
		List<Megachunk> optimizedChunks = new ArrayList<>();
		
		Megachunk mega = null;

		for (RawBsonDocument chunk : chunksCache.values()) {

			String ns = chunk.getString("ns").getValue();
			if (config.filterCheck(ns)) {
				continue;
			}
			
			String shard = chunk.getString("shard").getValue();
			String chunkId = shardClient.getIdFromChunk(chunk);
			
			if (mega == null || !ns.equals(mega.getNs()) || !shard.equals(mega.getShard())) {
				if (mega != null) {
					if (!ns.equals(mega.getNs())) {
						mega.setLast(true);
					}
					optimizedChunks.add(mega);
				}
				mega = new Megachunk();
				mega.setChunkId(chunkId);
				mega.setNs(ns);
				mega.setShard(shard);	
			} 

			BsonDocument min = (BsonDocument)chunk.get("min");
			if (mega.getMin() == null) {
				mega.setMin(min);
			}
			BsonDocument max = (BsonDocument)chunk.get("max");
			mega.addMax(max);
		}
		if (mega != null) {
			mega.setLast(true);
			optimizedChunks.add(mega);
		}
		return optimizedChunks;
	}
	
	/**
	 * Create chunks and move them, using the "optimized" method to reduce the total
	 * number of chunk moves required.
	 */
	public void createAndMoveChunks() {
		boolean doMove = true;

		logger.debug("createAndMoveChunks (optimized) started");
		//logger.debug("chunkQuery: {}", chunkQuery);

		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(sourceChunkQuery);
		if (destChunkQuery == null) {
			initializeDestChunkQuery();
		}
		Set<String> destMins = getChunkMins(destChunkQuery);
		
		double totalChunks = (double)sourceChunksCache.size();

		// step 1: build a list of "megachunks", each representing a range of consecutive chunks
		// that reside on the same shard. See Megachunk inner class.
		List<Megachunk> optimizedChunks = getMegaChunks(sourceChunksCache, sourceShardClient);
		logger.debug(String.format("optimized chunk count: %s", optimizedChunks.size()));
		
		int chunkCount = optimizedChunks.size() / 2;
		
		long ts;
		long startTsSeconds = Instant.now().getEpochSecond();

		// step 2: create splits for each of the megachunks, wherever they reside
		for (Megachunk mega2 : optimizedChunks) {
			if (!mega2.isLast()) {
				String megaHash = ((RawBsonDocument) mega2.getMax()).toJson();
				String megaId = String.format("%s_%s", mega2.getNs(), megaHash);
				
				if (! destMins.contains(megaId)) {
					destShardClient.splitAt(mega2.getNs(), mega2.getMax(), true);
				}
				chunkCount++;
				
				ts = Instant.now().getEpochSecond();
				long secondsSinceLastLog = ts - startTsSeconds;
				if (secondsSinceLastLog >= 60) {
					printChunkStatus(chunkCount, optimizedChunks.size(), "optimized chunks created");
					startTsSeconds = ts;
				}
			}
		}
		
		logger.debug("phase 2 complete, {} optimized chunks created", chunkCount);
		
		initializeDestChunkQuery();

		// get current locations of megachunks on destination
		Map<String, String> destChunkToShardMap = readDestinationChunks();
		destMins = getChunkMins(destChunkQuery);

		int errorCount = 0;
		int moveCount = 0;
		startTsSeconds = Instant.now().getEpochSecond();

		// step 3: move megachunks to correct shards
		for (Megachunk mega2 : optimizedChunks) {

			String mappedShard = getShardMapping(mega2.getShard());
			if (mappedShard == null) {
				throw new IllegalArgumentException(
						"No destination shard mapping found for source shard: " + mega2.getShard());
			}
			
			String destShard = destChunkToShardMap.get(mega2.getId());
			
			logger.debug("chunk shard: {}, mappedShard: {}, destShard: {}", mega2.getShard(), mappedShard, destShard);

			if (doMove && destShard != null && !mappedShard.equals(destShard)) {
				boolean moveSuccess = destShardClient.moveChunk(mega2.getNs(), (RawBsonDocument)mega2.getMin(), 
						(RawBsonDocument)mega2.getMax(), mappedShard, false, false, false, false);
				if (! moveSuccess) {
					errorCount++;
				} else {
					moveCount++;
				}
			}
			
			ts = Instant.now().getEpochSecond();
			long secondsSinceLastLog = ts - startTsSeconds;
			if (secondsSinceLastLog >= 60) {
				printChunkStatus(moveCount, optimizedChunks.size(), "chunks moved");
				startTsSeconds = ts;
			}
		}
		
		logger.debug("phase 3 complete, {} chunks moved", moveCount);

		// step 4: split megachunks into final chunks
		startTsSeconds = Instant.now().getEpochSecond();
		
		for (Megachunk mega2 : optimizedChunks) {
			for (BsonDocument mid : mega2.getMids()) {
				//getChunkMinKey
				String midHash = ((RawBsonDocument) mid).toJson();
				String midId = String.format("%s_%s", mega2.getNs(), midHash);
				if (! destMins.contains(midId)) {
					destShardClient.splitAt(mega2.getNs(), mid, true);
				}
				chunkCount++;
				ts = Instant.now().getEpochSecond();
				long secondsSinceLastLog = ts - startTsSeconds;
				if (secondsSinceLastLog >= 60) {
					printChunkStatus(chunkCount, totalChunks, "chunks created");
					startTsSeconds = ts;
				}
			}
		}
		printChunkStatus(chunkCount, totalChunks, "chunks created");
		logger.debug("createAndMoveChunks complete");
	}
	
	private void printChunkStatus(int chunkCount, double totalChunks, String opType) {
		double pctComplete = chunkCount/totalChunks * 100.;
		logger.debug(String.format("%.1f %% of %s ( %,d / %,.0f )", pctComplete, opType, chunkCount, totalChunks));
	}
	
	/**
	 * Create chunks on the dest side using the "split" runCommand NOTE that this
	 * will be very slow b/c of the locking process that happens with each chunk
	 */
	public void createDestChunksUsingSplitCommand() {
		logger.debug("createDestChunksUsingSplitCommand started");
		logger.debug("chunkQuery: {}", sourceChunkQuery);
		
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(sourceChunkQuery);
		destShardClient.loadChunksCache(destChunkQuery);

		String lastNs = null;
		int currentCount = 0;

		for (RawBsonDocument chunk : sourceChunksCache.values()) {

			String ns = chunk.getString("ns").getValue();
			if (config.filterCheck(ns)) {
				continue;
			}
			
			destShardClient.createChunk(chunk, true, true);
			//nsComplete = updateChunkCompletionStatus(chunk, ns);
			currentCount++;
			if (!ns.equals(lastNs) && lastNs != null) {
				logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
				currentCount = 0;
			}
			lastNs = ns;
		}
		logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
		logger.debug("createDestChunksUsingSplitCommand complete");
	}
	
	public void compareAndMoveChunks(boolean doMove, boolean ignoreMissing) {

		if (destChunkQuery == null) {
			initializeDestChunkQuery();
		}
		Map<String, String> destChunkMap = readDestinationChunks();
		logger.debug("chunkQuery: {}", sourceChunkQuery);
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(sourceChunkQuery);
		
		destShardClient.loadChunksCache(destChunkQuery);

		String lastNs = null;
		int currentCount = 0;
		int movedCount = 0;
		int mismatchedCount = 0;
		int matchedCount = 0;
		int missingCount = 0;
		int sourceTotalCount = 0;
		int errorCount = 0;

		for (RawBsonDocument sourceChunk : sourceChunksCache.values()) {
			sourceTotalCount++;
			String sourceId = sourceShardClient.getIdFromChunk(sourceChunk);
			//logger.debug("source id: " + sourceId);
			
			String sourceNs = sourceChunk.getString("ns").getValue();
			Namespace sourceNamespace = new Namespace(sourceNs);
			if (config.filterCheck(sourceNamespace)) {
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
			String mappedShard = getShardMapping(sourceShard);
			//String mappedShard = sourceToDestShardMap.get(sourceShard);
			if (mappedShard == null) {
				throw new IllegalArgumentException(
						"No destination shard mapping found for source shard: " + sourceShard);
			}
			
			//String sourceId = sourceChunk.getString("_id").getValue();
			String destShard = destChunkMap.get(sourceId);

			if (destShard == null && !ignoreMissing) {
				logger.error("Chunk with _id " + sourceId + " not found on destination");
				missingCount++;

			} else if (doMove && !mappedShard.equals(destShard)) {
				//logger.debug(String.format("%s: moving chunk from %s to %s", sourceNs, destShard, mappedShard));
				if (doMove) {
					boolean moveSuccess = destShardClient.moveChunk(sourceNs, sourceMin, sourceMax, mappedShard, ignoreMissing, false, false, false);
					if (! moveSuccess) {
						errorCount++;
					}
				}

				movedCount++;

			} else if (!doMove) {
				if (!mappedShard.equals(destShard)) {
					logger.warn(String.format("mismatch: %s ==> %s", destShard, mappedShard));
					logger.warn("dest chunk is on wrong shard for sourceChunk: " + sourceChunk);
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
	
	public void compareChunksEquivalent() {
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(sourceChunkQuery);
		Map<String, RawBsonDocument> destChunksCache = destShardClient.loadChunksCache(destChunkQuery);
		
		List<Megachunk> sourceMega = getMegaChunks(sourceChunksCache, sourceShardClient);
		
		List<Megachunk> destMega = getMegaChunks(destChunksCache, destShardClient);
		
		for (Megachunk m : sourceMega) {
			String sourceShard = m.getShard();
			String mappedShard = getShardMapping(sourceShard);
			m.setShard(mappedShard);
		}
		
		
		List<Megachunk> diff1 = new ArrayList<>(Sets.difference(Sets.newHashSet(sourceMega), Sets.newHashSet(destMega)));
		List<Megachunk> diff2 = new ArrayList<>(Sets.difference(Sets.newHashSet(destMega), Sets.newHashSet(sourceMega)));
		
		
		logger.debug("compareChunksEquivalent: source mega count: {}, dest mega count: {}", sourceMega.size(), destMega.size());
		logger.debug("diff1: {}", diff1);
		logger.debug("diff2: {}", diff2);
		
	}
	
	private Map<String, String> readDestinationChunks() {
		logger.debug("Reading destination chunks");
		Map<String, String> destChunkMap = new HashMap<String, String>();
		
		Map<String, RawBsonDocument> destChunks = destShardClient.getChunksCache(destChunkQuery);
		
		for (RawBsonDocument destChunk : destChunks.values()) {
			String id = destShardClient.getIdFromChunk(destChunk);
			//logger.debug("dest id: " + id);
			String shard = destChunk.getString("shard").getValue();
			destChunkMap.put(id, shard);
		}
		logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
		return destChunkMap;
	}
	
	private String getChunkMinKey(RawBsonDocument chunk) {
		RawBsonDocument min = (RawBsonDocument) chunk.get("min");
		String ns = chunk.getString("ns").getValue();
		String minHash = ((RawBsonDocument) min).toJson();
		String id = String.format("%s_%s", ns, minHash);
		return id;
	}
	
	private Set<String> getChunkMins(BsonDocument chunkQuery) {
		Set<String> minsSet = new HashSet<>();
		
		if (chunkQuery == null) {
			this.initializeDestChunkQuery();
		}
		
		//MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();
		//FindIterable<RawBsonDocument> destChunks = destChunksColl.find().sort(destShardClient.getChunkSort());
		Map<String, RawBsonDocument> destChunksMap = destShardClient.getChunksCache(chunkQuery);

		for (RawBsonDocument chunk : destChunksMap.values()) {
			minsSet.add(getChunkMinKey(chunk));
		}
		return minsSet;
	}
	
	public BsonDocument initializeSourceChunkQuery() {
		logger.debug("*** initializeSourceChunkQuery()");
		this.sourceChunkQuery = newChunkQuery(sourceShardClient);
		return sourceChunkQuery;
	}
	
	public BsonDocument initializeDestChunkQuery() {
		this.destChunkQuery = newChunkQuery(destShardClient);
		logger.debug("initializeDestChunkQuery(), query: {}", destChunkQuery);
		return destChunkQuery;
	}
	
	public BsonDocument newChunkQuery(ShardClient shardClient) {
		return newChunkQuery(shardClient, null);
	}
	
	public List<BsonBinary> fetchUUIDs(ShardClient shardClient, Set<String> dbNames) {
        List<BsonBinary> uuids = new ArrayList<>();
        MongoCollection<Document> collections = shardClient.getCollection("config.collections");
        
        List<Document> pipeline = Arrays.asList(
                new Document("$project", new Document("_id", 1)
                    .append("uuid", 1)
                    .append("dbName", new Document("$arrayElemAt", Arrays.asList(new Document("$split", Arrays.asList("$_id", ".")), 0)))),
                new Document("$match", new Document("dbName", new Document("$in", dbNames))),
                new Document("$project", new Document("_id", 1).append("uuid", 1))
            );
        
        collections.aggregate(pipeline).forEach(document -> {
            Object uuidObj = document.get("uuid");
            if (uuidObj instanceof UUID) {
            	BsonBinary bb = BsonUuidUtil.uuidToBsonBinary((UUID) uuidObj);
            	
            	String base64 = Base64.encode(bb.getData());

            	logger.debug("Adding collection: {}, bsonBinary: {}", document, base64);
            	uuids.add(bb);
                
            }
        });

        return uuids;
    }
	
	public BsonDocument newChunkQuery(ShardClient shardClient, String namespace) {
		BsonDocument chunkQuery = new BsonDocument();
		
		if (! config.getSourceShards().isEmpty()) {
			chunkQuery.append("shard", new BsonDocument("$in", BsonUtils.convertSetToBsonArray(shardClient.getShardsMap().keySet())));
		}
		if (namespace != null) {
			if (shardClient.isVersion5OrLater()) {
				BsonBinary uuidBinary = shardClient.getUuidForNamespace(namespace);
				chunkQuery.append("uuid", new BsonDocument("$eq", uuidBinary));
			} else {
				chunkQuery.append("ns", new BsonDocument("$eq", new BsonString(namespace)));
			}
			
		} else if (config.getIncludeNamespaces().size() > 0 || config.getIncludeDatabases().size() > 0) {
			List<BsonValue> inList = new ArrayList<>();
			List<BsonDocument> orList = new ArrayList<>();
			for (Namespace includeNs : config.getIncludeNamespaces()) {
				if (shardClient.isVersion5OrLater()) {
					BsonBinary uuidBinary = shardClient.getUuidForNamespace(includeNs.getNamespace());
					logger.debug("adding namespace {}, uuid: {}", includeNs, BsonUuidUtil.convertBsonBinaryToUuid(uuidBinary));
					inList.add(uuidBinary);
				} else {
					inList.add(new BsonString(includeNs.getNamespace()));
				}
			}
			if (shardClient.isVersion5OrLater()) {
				inList.addAll(fetchUUIDs(shardClient, config.getIncludeDatabases()));
				
			} else {
				for (String dbName : config.getIncludeDatabases()) {
					orList.add(new BsonDocument("ns", new BsonDocument("$regex", new BsonString("^" + dbName + "\\."))));
				}
			}
			
			BsonDocument inDoc;
			if (shardClient.isVersion5OrLater()) {
				inDoc = new BsonDocument("uuid", new BsonDocument("$in", new BsonArray(inList)));
			} else {
				inDoc = new BsonDocument("ns", new BsonDocument("$in", new BsonArray(inList)));
			}
			
			orList.add(inDoc);
			chunkQuery.append("$or", new BsonArray(orList));
		} else {
			if (shardClient.isVersion5OrLater()) {
				Document coll = shardClient.getCollectionsMap().get("config.system.sessions");
				if (coll != null) {
					UUID uuid = (UUID)coll.get("uuid");
					BsonBinary uuidBinary = BsonUuidUtil.uuidToBsonBinary(uuid);
					chunkQuery.append("uuid", new BsonDocument("$ne", uuidBinary));
				}
			} else {
				chunkQuery.append("ns", new BsonDocument("$ne", new BsonString("config.system.sessions")));
			}
		}
		return chunkQuery;
	}

	public String getShardMapping(String sourceShardName) {
		if (! altSourceToDestShardMap.isEmpty()) {
			String newKey = altSourceToDestShardMap.get(sourceShardName);
			return newKey;
			
		} else {
			return sourceToDestShardMap.get(sourceShardName);
		}
	}
	
	public String getDestToSourceShardMapping(String destShardName) {
		return destToSourceShardMap.get(destShardName);
	}

	public void setSourceChunkQuery(BsonDocument chunkQuery) {
		this.sourceChunkQuery = chunkQuery;
	}
	
	public Set<String> getShardsForNamespace(Namespace ns) {
		MongoCollection<Document> destChunksColl = destShardClient.getChunksCollection();
		Bson query = eq("ns", ns.getNamespace());
		Set<String> shards = new HashSet<>();
		destChunksColl.distinct("shard", query, String.class).into(shards);
		return shards;
}

	public BsonDocument getSourceChunkQuery() {
		return sourceChunkQuery;
	}

	public void setSourceShardClient(ShardClient sourceShardClient) {
		this.sourceShardClient = sourceShardClient;
	}
}
