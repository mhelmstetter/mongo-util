package com.mongodb.diff3;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.CodecUtils;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

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


    protected void computeDiff() {
        loadSourceDocs();
        loadDestDocs();

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
            result.onlyOnDest = diff.entriesOnlyOnRight().size();
        }
        result.bytesProcessed = Math.max(sourceBytesProcessed.longValue(), destBytesProcessed.longValue());
        long diffTime = System.currentTimeMillis() - compStart;
        logger.debug("Computed diff in {} ms[{}]", diffTime, Thread.currentThread().getName());
    }

    protected void loadSourceDocs() {
        long loadStart = System.currentTimeMillis();
        MongoClient shardClient = sourceShardClient.getShardMongoClient(srcShardName);
		MongoCollection<RawBsonDocument> sourceColl = getRawCollection(shardClient, namespace);
        sourceCursor = sourceColl.find(query).batchSize(10000).iterator();
        sourceDocs = loadDocs(sourceCursor, sourceBytesProcessed);
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("Loaded {} source docs for {} in {} ms[{}--{}]", sourceDocs.size(), namespace, loadTime,
                Thread.currentThread().getName(), srcShardName);
    }

    protected void loadDestDocs() {
        long loadStart = System.currentTimeMillis();
        MongoClient shardClient = destShardClient.getShardMongoClient(destShardName);
		MongoCollection<RawBsonDocument> destColl = getRawCollection(shardClient, namespace);
        destCursor = destColl.find(query).batchSize(10000).iterator();
        destDocs = loadDocs(destCursor, destBytesProcessed);
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("Loaded {} dest docs for {} in {} ms[{}--{}]", destDocs.size(), namespace, loadTime,
                Thread.currentThread().getName(), destShardName);
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

    protected static void closeCursor(MongoCursor<RawBsonDocument> cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception e) {
        }

    }

    protected MongoCollection<RawBsonDocument> getRawCollection(MongoClient client, Namespace ns) {
        return client.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), RawBsonDocument.class);
    }
}