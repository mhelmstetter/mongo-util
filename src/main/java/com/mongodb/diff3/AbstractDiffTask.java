package com.mongodb.diff3;

import com.google.common.base.Equivalence;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

public class AbstractDiffTask {

    protected static final Logger logger = LoggerFactory.getLogger(ShardedDiffTask.class);
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected DiffConfiguration config;
    protected String srcShardName;
    protected String destShardName;

    protected Namespace namespace;
    protected Bson query;
    protected long start;

    protected LongAdder sourceBytesProcessed = new LongAdder();
    protected LongAdder destBytesProcessed = new LongAdder();

    protected MongoCursor<RawBsonDocument> sourceCursor = null;
    protected MongoCursor<RawBsonDocument> destCursor = null;

    protected Map<String, String> sourceDocs = null;
    protected Map<String, String> destDocs = null;

    protected DiffResult result;
    protected String chunkString = "[:]";


    protected void computeDiff() {
        loadSourceDocs(null);
        loadDestDocs(null);

        doComparison();
    }

    protected void computeDiff(List<String> ids) {
        loadSourceDocs(ids);
        loadDestDocs(ids);
        doComparison();
    }

    private void doComparison() {
        long compStart = System.currentTimeMillis();
        MapDifference<String, String> diff = Maps.difference(sourceDocs, destDocs);

        if (diff.areEqual()) {
            int numMatches = sourceDocs.size();
            result.matches = numMatches;
        } else {
            Map<String, ValueDifference<String>> valueDiff = diff.entriesDiffering();
            int numMatches = sourceDocs.size() - valueDiff.size();
            result.matches = numMatches;
            for (Iterator<?> it = valueDiff.entrySet().iterator(); it.hasNext(); ) {
                @SuppressWarnings("unchecked")
                Map.Entry<String, ValueDifference<String>> entry = (Map.Entry<String, ValueDifference<String>>) it.next();
                String key = entry.getKey();
                result.addFailedKey(key);
            }
            result.onlyOnSource = diff.entriesOnlyOnLeft().size();
            for (String id : diff.entriesOnlyOnLeft().keySet()) {
                result.addFailedKey(id);
            }
            result.onlyOnDest = diff.entriesOnlyOnRight().size();
            for (String id : diff.entriesOnlyOnRight().keySet()) {
                result.addFailedKey(id);
            }
        }
        result.bytesProcessed = Math.max(sourceBytesProcessed.longValue(), destBytesProcessed.longValue());
        long diffTime = System.currentTimeMillis() - compStart;
        logger.debug("[{}] computed diff in {} ms ({}-{})",
                Thread.currentThread().getName(), diffTime, namespace.getNamespace(), chunkString);
    }

    protected void loadSourceDocs(List<String> ids) {
        long loadStart = System.currentTimeMillis();
        MongoClient shardClient = sourceShardClient.getShardMongoClient(srcShardName);
        MongoCollection<RawBsonDocument> sourceColl = getRawCollection(shardClient, namespace);
        Bson q = (ids != null && ids.size() > 0) ? formIdsQuery(ids) : query;
        sourceCursor = sourceColl.find(q).iterator();
        sourceDocs = loadDocs(sourceCursor, sourceBytesProcessed);
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("[{}] loaded {} source docs for {} in {} ms ({})",
                Thread.currentThread().getName(), sourceDocs.size(),
                namespace, loadTime, chunkString);
    }

    protected void loadDestDocs(List<String> ids) {
        long loadStart = System.currentTimeMillis();
        MongoClient shardClient = destShardClient.getShardMongoClient(destShardName);
        MongoCollection<RawBsonDocument> destColl = getRawCollection(shardClient, namespace);
        Bson q = (ids != null && ids.size() > 0) ? formIdsQuery(ids) : query;
        destCursor = destColl.find(q).iterator();
        destDocs = loadDocs(destCursor, destBytesProcessed);
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("[{}] loaded {} dest docs for {} in {} ms ({})",
                Thread.currentThread().getName(), destDocs.size(),
                namespace, loadTime, chunkString);
    }

    protected Map<String, String> loadDocs(MongoCursor<RawBsonDocument> cursor, LongAdder byteCounter) {
        Map<String, String> docs = new LinkedHashMap<>();
        while (cursor.hasNext()) {
            RawBsonDocument doc = cursor.next();
            String id = doc.get("_id").toString();
            byte[] docBytes = doc.getByteBuffer().array();
            byteCounter.add(docBytes.length);

            String docHash = CodecUtils.md5Hex(docBytes);

            docs.put(id, docHash);
        }
        return docs;
    }

    protected long timeSpent(long stop) {
        return stop - start;
    }

    protected void closeCursor(MongoCursor<RawBsonDocument> cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception e) {
        }
    }

    protected Bson formIdsQuery(List<String> ids) {
        List<Document> idDocs = ids.stream().map(i -> Document.parse(i)).collect(Collectors.toList());
        return Filters.in("_id", idDocs);
    }

    protected MongoCollection<RawBsonDocument> getRawCollection(MongoClient client, Namespace ns) {
        return client.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), RawBsonDocument.class);
    }
}