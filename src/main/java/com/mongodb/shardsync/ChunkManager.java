package com.mongodb.shardsync;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Namespace;

public class ChunkManager {

	private static Logger logger = LoggerFactory.getLogger(ChunkManager.class);
	
	private Document chunkQuery;
	private ShardClient destShardClient;
	private ShardClient sourceShardClient;
	private boolean filtered;
	private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	private Set<String> includeDatabases = new HashSet<String>();
	
	private DocumentCodec codec = new DocumentCodec();
	private DecoderContext decoderContext = DecoderContext.builder().build();
	
	private List<Document> optimizedChunks = new LinkedList<>();

	/**
	 * Create chunks and move them, using the "optimized" method to reduce the total
	 * number of chunk moves required.
	 */
	public void createAndMoveChunks() {

		logger.debug("createAndMoveChunks (optimized) started");
		logger.debug("chunkQuery: {}", chunkQuery);

		Map<String, RawBsonDocument> sourceChunksCache = sourceShardClient.loadChunksCache(chunkQuery);
		destShardClient.loadChunksCache(chunkQuery);

		String lastNs = null;
		String lastShard = null;
		Document lastChunk = null;
		int currentCount = 0;

		for (RawBsonDocument chunk : sourceChunksCache.values()) {

			String ns = chunk.getString("ns").getValue();
			if (filterCheck(ns)) {
				continue;
			}
			
			String shard = chunk.getString("shard").getValue();
			
			if (ns.equals(lastNs) && shard.equals(lastShard)) {
				BsonDocument max = (BsonDocument) chunk.get("max");
				lastChunk.put("max", max);
				
			} else {
				lastChunk = codec.decode(chunk.asBsonReader(), decoderContext);
				optimizedChunks.add(lastChunk);
			}
			
			currentCount++;
			if (!ns.equals(lastNs) && lastNs != null) {
				logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
				currentCount = 0;
			}
			lastNs = ns;
			lastShard = shard;
			
		}
		logger.debug(String.format("optimized chunk count: %s", optimizedChunks.size()));
		//logger.debug(String.format("%s - created %s chunks", lastNs, currentCount));
		logger.debug("createAndMoveChunks complete");

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
		if (ns.getCollectionName().equals("system.profile") || ns.getCollectionName().equals("system.users")) {
			return true;
		}
		return false;
	}

	public void setChunkQuery(Document chunkQuery) {
		this.chunkQuery = chunkQuery;
	}

	public void setDestShardClient(ShardClient destShardClient) {
		this.destShardClient = destShardClient;
	}

	public void setSourceShardClient(ShardClient sourceShardClient) {
		this.sourceShardClient = sourceShardClient;
	}

	public void setFiltered(boolean filtered) {
		this.filtered = filtered;
	}

	public void setIncludeNamespaces(Set<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}

	public void setIncludeDatabases(Set<String> includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

}
