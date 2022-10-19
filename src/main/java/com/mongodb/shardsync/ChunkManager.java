package com.mongodb.shardsync;

import static com.mongodb.client.model.Filters.regex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import com.mongodb.model.Shard;
import com.mongodb.shardsync.ShardClient.ShardClientType;

public class ChunkManager {

	private static Logger logger = LoggerFactory.getLogger(ChunkManager.class);
	
	private Document chunkQuery;
	private ShardClient destShardClient;
	private ShardClient sourceShardClient;
	
	
	private DocumentCodec codec = new DocumentCodec();
	private DecoderContext decoderContext = DecoderContext.builder().build();
	
	private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();
	private Map<String, String> destToSourceShardMap = new HashMap<String, String>();
	
	private Map<String, String> altSourceToDestShardMap = new HashMap<String, String>();
	
	private List<Megachunk> optimizedChunks = new LinkedList<>();
	
	private SyncConfiguration config;
	
	public ChunkManager (SyncConfiguration config) {
		this.config = config;
	}
	
	@SuppressWarnings("unchecked")
	public void initalize() {
		initializeChunkQuery();
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
			
			sourceShardClient.setRsPattern(config.getSourceRsPattern());
			destShardClient.setRsPattern(config.getDestRsPattern());
			sourceShardClient.setRsStringsManual(config.getSourceRsManual());
			destShardClient.setRsStringsManual(config.getDestRsManual());
			destShardClient.setCsrsUri(config.getDestCsrsUri());
			
			sourceShardClient.init();
			destShardClient.init();
			
		} else {
			logger.debug("Default 1:1 shard mapping");
			
			sourceShardClient = new ShardClient("source", source, null, shardClientType);
			config.setSourceShardClient(sourceShardClient);
			destShardClient = new ShardClient("dest", dest, null, shardClientType);
			config.setDestShardClient(destShardClient);
			sourceShardClient.setRsPattern(config.getSourceRsPattern());
			destShardClient.setRsPattern(config.getDestRsPattern());
			sourceShardClient.setRsStringsManual(config.getSourceRsManual());
			destShardClient.setRsStringsManual(config.getDestRsManual());
			destShardClient.setCsrsUri(config.getDestCsrsUri());
			
			sourceShardClient.init();
			destShardClient.init();
			
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
		// reverse map
		destToSourceShardMap = MapUtils.invertMap(sourceToDestShardMap);
		
		if (config.getSourceClusterPattern() == null && ! sourceShardClient.isMongos()) {
			throw new IllegalArgumentException("source connection must be to a mongos router");
		}
	}
	
	/**
	 * Create chunks and move them, using the "optimized" method to reduce the total
	 * number of chunk moves required.
	 */
	public void createAndMoveChunks() {
		boolean doMove = true;

		logger.debug("createAndMoveChunks (optimized) started");
		logger.debug("chunkQuery: {}", chunkQuery);

		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);
		destShardClient.loadChunksCache(chunkQuery);

		// step 1: build a list of "megachunks", each representing a range of consecutive chunks
		// that reside on the same shard. See Megachunk inner class.
		Megachunk mega = null;

		for (RawBsonDocument chunk : sourceChunksCache.values()) {

			String ns = chunk.getString("ns").getValue();
			if (config.filterCheck(ns)) {
				continue;
			}
			
			String shard = chunk.getString("shard").getValue();
			
			if (mega == null || !ns.equals(mega.getNs()) || !shard.equals(mega.getShard())) {
				if (mega != null) {
					optimizedChunks.add(mega);
				}
				mega = new Megachunk();
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
			optimizedChunks.add(mega);
		}
		logger.debug(String.format("optimized chunk count: %s", optimizedChunks.size()));

		// step 2: create splits for each of the megachunks, wherever they reside
		for (Megachunk mega2 : optimizedChunks) {
			destShardClient.splitAt(mega2.getNs(), mega2.getMax(), true);
		}

		// get current locations of megachunks on destination
		Map<String, String> destChunkMap = readDestinationChunks();

		int errorCount = 0;

		// step 3: move megachunks to correct shards
		for (Megachunk mega2 : optimizedChunks) {

			String mappedShard = getShardMapping(mega2.getShard());
			//String mappedShard = sourceToDestShardMap.get(sourceShard);
			if (mappedShard == null) {
				throw new IllegalArgumentException(
						"No destination shard mapping found for source shard: " + mega2.getShard());
			}

			String destShard = destChunkMap.get(mega2.getId());

			if (destShard == null) {
				logger.error("Chunk with _id " + mega2.getId() + " not found on destination");
			} else if (doMove && !mappedShard.equals(destShard)) {
				//logger.debug(String.format("%s: moving chunk from %s to %s", sourceNs, destShard, mappedShard));
				if (doMove) {
					boolean moveSuccess = destShardClient.moveChunk(mega2.getShard(), (RawBsonDocument)mega2.getMin(), (RawBsonDocument)mega2.getMax(), mappedShard, false);
					if (! moveSuccess) {
						errorCount++;
					}
				}
			}
		}

		// step 4: split megachunks into final chunks
		for (Megachunk mega2 : optimizedChunks) {
			for (BsonDocument mid : mega2.getMids()) {
				destShardClient.splitAt(mega2.getNs(), mid, true);
			}
		}

		//logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
		logger.debug("createAndMoveChunks complete");
	}
	
	/**
	 * Create chunks on the dest side using the "split" runCommand NOTE that this
	 * will be very slow b/c of the locking process that happens with each chunk
	 */
	public void createDestChunksUsingSplitCommand() {
		logger.debug("createDestChunksUsingSplitCommand started");
		logger.debug("chunkQuery: {}", chunkQuery);
		
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);
		destShardClient.loadChunksCache(chunkQuery);

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

		Map<String, String> destChunkMap = readDestinationChunks();
		logger.debug("chunkQuery: {}", chunkQuery);
		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);
		destShardClient.loadChunksCache(chunkQuery);

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
			String sourceId = ShardClient.getIdFromChunk(sourceChunk);
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
					boolean moveSuccess = destShardClient.moveChunk(sourceNs, sourceMin, sourceMax, mappedShard, ignoreMissing);
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
	
	private Map<String, String> readDestinationChunks() {
		logger.debug("Reading destination chunks");
		Map<String, String> destChunkMap = new HashMap<String, String>();
		MongoCollection<RawBsonDocument> destChunksColl = destShardClient.getChunksCollectionRaw();
		FindIterable<RawBsonDocument> destChunks = destChunksColl.find().sort(Sorts.ascending("ns", "min"));

		for (RawBsonDocument destChunk : destChunks) {
			String id = ShardClient.getIdFromChunk(destChunk);
			//logger.debug("dest id: " + id);
			String shard = destChunk.getString("shard").getValue();
			destChunkMap.put(id, shard);
		}
		logger.debug("Done reading destination chunks, count = " + destChunkMap.size());
		return destChunkMap;
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Document initializeChunkQuery() {
		chunkQuery = new Document();
		if (config.getIncludeNamespaces().size() > 0 || config.getIncludeDatabases().size() > 0) {
			List inList = new ArrayList();
			List orList = new ArrayList();
			// Document orDoc = new Document("$or", orList);
			chunkQuery.append("$or", orList);
			Document inDoc = new Document("ns", new Document("$in", inList));
			orList.add(inDoc);
			// orDoc.append("ns", inDoc);
			for (Namespace includeNs : config.getIncludeNamespaces()) {
				inList.add(includeNs.getNamespace());
			}
			for (String dbName : config.getIncludeDatabases()) {
				orList.add(regex("ns", "^" + dbName + "\\."));
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

	public void setChunkQuery(Document chunkQuery) {
		this.chunkQuery = chunkQuery;
	}


	class Megachunk {
		private String ns = null;
		private String shard = null;
		private BsonDocument min = null;
		private List<BsonDocument> mids = new LinkedList<>();
		private BsonDocument max = null;

		public BsonDocument getMin() {
			return min;
		}
		public void setMin(BsonDocument min) {
			this.min = min;
		}

		public List<BsonDocument> getMids() {
			return mids;
		}
		public void setMids(List<BsonDocument> mids) {
			this.mids = mids;
		}

		public BsonDocument getMax() {
			return max;
		}
		public void addMax(BsonDocument max) {
			if (this.max != null) {
				this.mids.add(this.max);
			}
			this.max = max;
		}

		public String getNs() {
			return ns;
		}
		public void setNs(String ns) {
			this.ns = ns;
		}

		public String getShard() {
			return shard;
		}
		public void setShard(String shard) {
			this.shard = shard;
		}

		public String getId() {
			String minHash = ((RawBsonDocument)min).toJson();
			String maxHash = ((RawBsonDocument)max).toJson();
			return String.format("%s_%s_%s", ns, minHash, maxHash);
		}
	}
}
