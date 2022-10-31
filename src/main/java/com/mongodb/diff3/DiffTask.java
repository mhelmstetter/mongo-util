package com.mongodb.diff3;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;

public class DiffTask implements Callable<DiffResult> {
	
	
	protected static final Logger logger = LoggerFactory.getLogger(DiffTask.class);
	
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected DiffConfiguration config;
    
    private RawBsonDocument chunk;
    
    
    protected Bson chunkQuery;
    
    
	
	public DiffTask(ShardClient sourceShardClient, ShardClient destShardClient, DiffConfiguration config, RawBsonDocument chunk) {
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.config = config;
        this.chunk = chunk;
    }

	@Override
	public DiffResult call() throws Exception {
		
		DiffResult result = new DiffResult();
		result.setChunk(chunk);
		MongoCursor<RawBsonDocument> sourceCursor = null;
		MongoCursor<RawBsonDocument> destCursor = null;
		
		BsonDocument min = chunk.getDocument("min");
		BsonDocument max = chunk.getDocument("max");
		String nsStr = chunk.getString("ns").getValue();
		Namespace ns = new Namespace(nsStr);
		
		MongoCollection<RawBsonDocument> sourceColl = sourceShardClient.getCollectionRaw(ns);
		MongoCollection<RawBsonDocument> destColl = destShardClient.getCollectionRaw(ns);
		
		Document shardCollection = sourceShardClient.getCollectionsMap().get(nsStr);
		Document shardKeysDoc = (Document) shardCollection.get("key");
		Set<String> shardKeys = shardKeysDoc.keySet();
		
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
		
		try {
			
			
			sourceCursor = sourceColl.find(chunkQuery).iterator();
			destCursor = destColl.find(chunkQuery).iterator();
			
			Map<BsonValue, String> sourceDocs = loadDocs(sourceCursor);
			Map<BsonValue, String> destDocs = loadDocs(destCursor);

			MapDifference<BsonValue, String> diff = Maps.difference(sourceDocs, destDocs);
			
			if (diff.areEqual()) {
				result.matches = sourceDocs.size();
			} else {
				Map<BsonValue, ValueDifference<String>> valueDiff = diff.entriesDiffering();
				for (Iterator<?> it = valueDiff.entrySet().iterator(); it.hasNext();) {
			        @SuppressWarnings("unchecked")
			        Map.Entry<BsonValue, ValueDifference<String>> entry = (Map.Entry<BsonValue, ValueDifference<String>>) it.next();
			        BsonValue key = entry.getKey();
			        result.addFailedKey(key);

			    }
			}
			
			
		} catch (Exception me) {
        	logger.error("fatal error diffing chunk, ns: {}", ns, me);
        	result = null;
        } finally {
        	closeCursor(sourceCursor);
        	closeCursor(destCursor);
		}
		
		return result;
	}
	
	private Map<BsonValue, String> loadDocs(MongoCursor<RawBsonDocument> cursor) {
		Map<BsonValue, String> docs = new LinkedHashMap<>();
		while (cursor.hasNext()) {
			RawBsonDocument doc = cursor.next();
			BsonValue id = doc.get("_id");
			byte[] docBytes = doc.getByteBuffer().array();
			String docHash = CodecUtils.md5Hex(docBytes);
			docs.put(id, docHash);
		}
		return docs;
	}

	
	private static void closeCursor(MongoCursor<RawBsonDocument> cursor) {
		try {
			if (cursor != null) {
				cursor.close();
        	}
		} catch (Exception e) {
		}
		
	}
	
}
