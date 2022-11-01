package com.mongodb.diff3;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;

import static com.mongodb.client.model.Filters.*;

public class UnshardedDiffTask implements Callable<DiffResult> {
    protected static final Logger logger = LoggerFactory.getLogger(UnshardedDiffTask.class);
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected String nsStr;

    public UnshardedDiffTask(ShardClient sourceShardClient, ShardClient destShardClient, String nsStr) {
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.nsStr = nsStr;
    }

    @Override
    public UnshardedDiffResult call() throws Exception {
        UnshardedDiffResult result = new UnshardedDiffResult();
        MongoCursor<RawBsonDocument> sourceCursor = null;
        MongoCursor<RawBsonDocument> destCursor = null;
        Namespace ns = new Namespace(nsStr);

        MongoCollection<RawBsonDocument> sourceColl = sourceShardClient.getCollectionRaw(ns);
        MongoCollection<RawBsonDocument> destColl = destShardClient.getCollectionRaw(ns);

        try {
            sourceCursor = sourceColl.find().iterator();
            destCursor = destColl.find().iterator();

            Map<BsonValue, String> sourceDocs = loadDocs(sourceCursor);
            Map<BsonValue, String> destDocs = loadDocs(destCursor);

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
