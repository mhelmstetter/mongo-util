package com.mongodb.diff3;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.MapDifference.ValueDifference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;

public class AbstractDiffTask {

	protected static final Logger logger = LoggerFactory.getLogger(ShardedDiffTask.class);
	protected ShardClient sourceShardClient;
	protected ShardClient destShardClient;
	protected DiffConfiguration config;
	
	protected Namespace namespace;
	protected Bson query;
	
	protected long sourceBytesProcessed;
	protected long destBytesProcessed;
	
	protected MongoCursor<RawBsonDocument> sourceCursor = null;
	protected MongoCursor<RawBsonDocument> destCursor = null;
    
	protected Map<BsonValue, String> sourceDocs = null;
	protected Map<BsonValue, String> destDocs = null;
	
	protected DiffResult result;


	protected void computeDiff() {
		MapDifference<BsonValue, String> diff = Maps.difference(sourceDocs, destDocs);

        if (diff.areEqual()) {
            int numMatches = sourceDocs.size();
            result.matches = numMatches;
        } else {
            Map<BsonValue, ValueDifference<String>> valueDiff = diff.entriesDiffering();
            int numMatches = sourceDocs.size() - valueDiff.size();
            result.matches = numMatches;
            for (Iterator<?> it = valueDiff.entrySet().iterator(); it.hasNext(); ) {
                @SuppressWarnings("unchecked")
                Map.Entry<BsonValue, ValueDifference<String>> entry = (Map.Entry<BsonValue, ValueDifference<String>>) it.next();
                BsonValue key = entry.getKey();
                result.addFailedKey(key);
            }
            result.onlyOnSource = diff.entriesOnlyOnLeft().size();
            result.onlyOnDest = diff.entriesOnlyOnRight().size();
        }
        result.bytesProcessed = Math.max(sourceBytesProcessed, destBytesProcessed);
	}
	
	protected Map<BsonValue, String> loadSourceDocs() {
		MongoCollection<RawBsonDocument> sourceColl = sourceShardClient.getCollectionRaw(namespace);
		sourceCursor = sourceColl.find(query).iterator();
		return loadDocs(sourceCursor, sourceBytesProcessed);
	}
	
	protected Map<BsonValue, String> loadDestDocs() {
		MongoCollection<RawBsonDocument> destColl = destShardClient.getCollectionRaw(namespace);
		destCursor = destColl.find(query).iterator();
		return loadDocs(destCursor, destBytesProcessed);
	}

	protected Map<BsonValue, String> loadDocs(MongoCursor<RawBsonDocument> cursor, long byteCounter) {
		Map<BsonValue, String> docs = new LinkedHashMap<>();
		while (cursor.hasNext()) {
			RawBsonDocument doc = cursor.next();
			BsonValue id = doc.get("_id");
			byte[] docBytes = doc.getByteBuffer().array();
			byteCounter += docBytes.length;
			String docHash = CodecUtils.md5Hex(docBytes);
			docs.put(id, docHash);
		}
		return docs;
	}
	
	protected static void closeCursor(MongoCursor<RawBsonDocument> cursor) {
		try {
			if (cursor != null) {
				cursor.close();
	    	}
		} catch (Exception e) {
		}
		
	}


}