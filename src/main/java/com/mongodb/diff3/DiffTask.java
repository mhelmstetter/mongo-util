package com.mongodb.diff3;

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
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.diff3.DiffTask.Target.DEST;
import static com.mongodb.diff3.DiffTask.Target.SOURCE;

public class DiffTask implements Callable<DiffResult> {


    protected enum Target {
        SOURCE("source"),
        DEST("dest");

        private final String name;

        Target(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    protected static final Logger logger = LoggerFactory.getLogger(DiffTask.class);
    protected ShardClient sourceShardClient;
    protected ShardClient destShardClient;
    protected DiffConfiguration config;
    protected String srcShardName;
    protected String destShardName;

    protected String namespace;
    protected Bson query;
    protected long start;
    protected RawBsonDocument chunk;

    protected LongAdder sourceBytesProcessed = new LongAdder();
    protected LongAdder destBytesProcessed = new LongAdder();

    protected MongoCursor<RawBsonDocument> sourceCursor = null;
    protected MongoCursor<RawBsonDocument> destCursor = null;

    protected Map<BsonValue, String> sourceDocs = null;
    protected Map<BsonValue, String> destDocs = null;
    protected String chunkString = "[:]";
    protected Queue<RetryTask> retryQueue;

    public DiffTask(ShardClient sourceShardClient, ShardClient destShardClient, DiffConfiguration config,
                    RawBsonDocument chunk, String namespace, String srcShardName,
                    String destShardName, Queue<RetryTask> retryQueue) {
        this.sourceShardClient = sourceShardClient;
        this.destShardClient = destShardClient;
        this.config = config;
        this.chunk = chunk;
        this.namespace = namespace;
        this.srcShardName = srcShardName;
        this.destShardName = destShardName;
        this.retryQueue = retryQueue;
    }


    @Override
    public DiffResult call() throws Exception {
        DiffResult result;
        start = System.currentTimeMillis();
        query = new BsonDocument();
        if (chunk != null) {
            query = formChunkQuery();
        }

        try {
            result = computeDiff();
        } catch (Exception e){
            logger.error("[{}] fatal error diffing ({}-{})",
                    Thread.currentThread().getName(), namespace, chunkString, e);
            throw new RuntimeException(e);
        } finally {
            closeCursor(sourceCursor);
            closeCursor(destCursor);
        }

        result.setNs(namespace);
        result.setChunkString(chunkString);

        if (result.getFailureCount() > 0) {
            RetryStatus retryStatus= new RetryStatus(0, System.currentTimeMillis());
            RetryTask retryTask = new RetryTask(
                    retryStatus, this, result, result.getFailedIds(), retryQueue);
            retryQueue.add(retryTask);
            logger.debug("[{}] detected {} failures and added a retry task ({}-{})",
                    Thread.currentThread().getName(), result.getFailureCount(), chunkString);
        } else {
            retryQueue.add(RetryTask.END_TOKEN);
        }

        long timeSpent = System.currentTimeMillis() - start;
        logger.debug("[{}] completed a diff task in {} ms ({}-{})",
                Thread.currentThread().getName(), timeSpent, namespace, chunkString);

        return result;
    }

    protected Bson formChunkQuery() {
        Bson query;
        BsonDocument min = chunk.getDocument("min");
        BsonDocument max = chunk.getDocument("max");
        chunkString = "[" + min.toString() + " : " + max.toString() + "]";

        Document shardCollection = sourceShardClient.getCollectionsMap().get(namespace);
        Document shardKeysDoc = (Document) shardCollection.get("key");
        Set<String> shardKeys = shardKeysDoc.keySet();

        if (shardKeys.size() > 1) {
            List<Bson> filters = new ArrayList<>(shardKeys.size());
            for (String key : shardKeys){
                BsonValue minkey = min.get(key);
                BsonValue maxkey = max.get(key);
                if (minkey.equals(maxkey)) {
                    filters.add(eq(key, minkey));
                } else {
                    filters.add(and(gte(key, minkey), lt(key, maxkey)));
                }
            }
            query = and(filters);
        } else {
            String key = shardKeys.iterator().next();
            query = and(gte(key, min.get(key)), lt(key, max.get(key)));
        }
        return query;
    }


    protected DiffResult computeDiff() {
        return computeDiff(null);
    }

    protected DiffResult computeDiff(Collection<BsonValue> ids) {
        sourceDocs = load(ids, SOURCE);
        destDocs = load(ids, DEST);
        return doComparison();
    }

    private DiffResult doComparison() {
        DiffResult result = new DiffResult();
        long compStart = System.currentTimeMillis();
        MapDifference<BsonValue, String> diff = Maps.difference(sourceDocs, destDocs);

        if (diff.areEqual()) {
            int numMatches = sourceDocs.size();
            result.setMatches(numMatches);
        } else {
            Map<BsonValue, ValueDifference<String>> valueDiff = diff.entriesDiffering();
            for (Iterator<?> it = valueDiff.entrySet().iterator(); it.hasNext(); ) {
                @SuppressWarnings("unchecked")
                Map.Entry<BsonValue, ValueDifference<String>> entry = (Map.Entry<BsonValue, ValueDifference<String>>) it.next();
                BsonValue key = entry.getKey();
                result.addFailedKey(key);
            }
            result.setOnlyOnSource(diff.entriesOnlyOnLeft().size());
            for (BsonValue id : diff.entriesOnlyOnLeft().keySet()) {
                result.addFailedKey(id);
            }
            result.setOnlyOnDest(diff.entriesOnlyOnRight().size());
            for (BsonValue id : diff.entriesOnlyOnRight().keySet()) {
                result.addFailedKey(id);
            }
            int numMatches = (int) (sourceDocs.size() - valueDiff.size()
                    - result.getOnlyOnSource() - result.getOnlyOnDest());
            result.setMatches(numMatches);
        }
        result.setBytesProcessed(Math.max(sourceBytesProcessed.longValue(), destBytesProcessed.longValue()));
        long diffTime = System.currentTimeMillis() - compStart;
        logger.debug("[{}] computed diff in {} ms ({}-{})",
                Thread.currentThread().getName(), diffTime, namespace, chunkString);
        return result;
    }

    protected Map<BsonValue, String> load(Collection<BsonValue> ids, Target target) {
        Map<BsonValue, String> output = new HashMap<>();
        long loadStart = System.currentTimeMillis();
        ShardClient shardClient;
        String shardName;
        LongAdder bytesProcessed;
        switch (target) {
            case SOURCE:
                shardClient = sourceShardClient;
                shardName = srcShardName;
                bytesProcessed = sourceBytesProcessed;
                break;
            case DEST:
                shardClient = destShardClient;
                shardName = destShardName;
                bytesProcessed = destBytesProcessed;
                break;
            default:
                throw new RuntimeException("Unexpected target type: " + target.getName());
        }
        MongoClient shardMongoClient = shardClient.getShardMongoClient(shardName);
        MongoCollection<RawBsonDocument> coll = getRawCollection(shardMongoClient, namespace);
        Bson q = (ids != null && ids.size() > 0) ? formIdsQuery(ids) : query;
        MongoCursor<RawBsonDocument> cursor = coll.find(q).batchSize(10000).iterator();

        while (cursor.hasNext()) {
            RawBsonDocument doc = cursor.next();
            BsonValue id = doc.get("_id");
            byte[] docBytes = doc.getByteBuffer().array();
            bytesProcessed.add(docBytes.length);

            String docHash = CodecUtils.md5Hex(docBytes);
            output.put(id, docHash);
        }
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("[{}] loaded {} {} docs for {} in {} ms ({})",
                Thread.currentThread().getName(), output.size(), target.getName(), namespace, loadTime, chunkString);
        return output;
    }

    protected void closeCursor(MongoCursor<RawBsonDocument> cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception e) {
        }
    }

    protected Bson formIdsQuery(Collection<BsonValue> ids) {
//        List<Document> idDocs = ids.stream().map(i -> Document.parse(i)).collect(Collectors.toList());
        return Filters.in("_id", ids);
    }

    protected MongoCollection<RawBsonDocument> getRawCollection(MongoClient client, String namespace) {
        Namespace ns = new Namespace(namespace);
        return client.getDatabase(ns.getDatabaseName()).getCollection(ns.getCollectionName(), RawBsonDocument.class);
    }

    public String getNamespace() {
        return namespace;
    }
}