package com.mongodb.diff3;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.diff3.partition.PartitionDiffTask;
import com.mongodb.model.Namespace;
import com.mongodb.util.CodecUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;

import static com.mongodb.diff3.DiffTask.Target.DEST;
import static com.mongodb.diff3.DiffTask.Target.SOURCE;

public abstract class DiffTask implements Callable<DiffResult> {


    public enum Target {
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
    protected DiffConfiguration config;

    protected Namespace namespace;
    protected Pair<Bson, Bson> bounds;
    protected long start;
    protected DiffSummary summary;

    protected LongAdder sourceBytesProcessed = new LongAdder();
    protected LongAdder destBytesProcessed = new LongAdder();

    protected MongoCursor<RawBsonDocument> sourceCursor = null;
    protected MongoCursor<RawBsonDocument> destCursor = null;

    protected Map<BsonValue, String> sourceDocs = null;
    protected Map<BsonValue, String> destDocs = null;
    protected Queue<RetryTask> retryQueue;

    public DiffTask(DiffConfiguration config, Namespace namespace,
                    Queue<RetryTask> retryQueue, DiffSummary summary) {
        this.config = config;
        this.namespace = namespace;
        this.retryQueue = retryQueue;
        this.summary = summary;
    }

    //    protected abstract Bson getDiffQuery();
    protected abstract Pair<Bson, Bson> getChunkBounds();

    protected abstract String unitLogString();

    protected abstract DiffResult initDiffResult();

    protected abstract RetryTask createRetryTask(RetryStatus retryStatus, DiffResult result);

    protected abstract RetryTask endToken();


    @Override
    public DiffResult call() throws Exception {
        DiffResult result;
        start = System.currentTimeMillis();
//        query = getDiffQuery();
        bounds = getChunkBounds();

        try {
            result = computeDiff();
        } catch (Exception e) {
            logger.error("[{}] fatal error diffing ({})",
                    Thread.currentThread().getName(), unitLogString(), e);
            throw new RuntimeException(e);
        } finally {
            closeCursor(sourceCursor);
            closeCursor(destCursor);
        }

        if (result.getFailureCount() > 0) {
            RetryStatus retryStatus = new RetryStatus(0, System.currentTimeMillis());
            RetryTask retryTask = createRetryTask(retryStatus, result);
            retryQueue.add(retryTask);
            logger.debug("[{}] detected {} failures and added a retry task ({})",
                    Thread.currentThread().getName(), result.getFailureCount(), unitLogString());
        } else {
            logger.debug("[{}] sending end token for ({})", Thread.currentThread().getName(),
                    result.unitLogString());
            retryQueue.add(endToken());
        }

        long timeSpent = System.currentTimeMillis() - start;
        logger.debug("[{}] completed a diff task in {} ms ({})",
                Thread.currentThread().getName(), timeSpent, unitLogString());

        return result;
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
        DiffResult result = initDiffResult();
        long compStart = System.currentTimeMillis();
        MapDifference<BsonValue, String> diff = Maps.difference(sourceDocs, destDocs);

        if (diff.areEqual()) {
            int numMatches = sourceDocs.size();
            result.setMatches(numMatches);
        } else {
            Map<BsonValue, ValueDifference<String>> valueDiff = diff.entriesDiffering();
            for (Map.Entry<BsonValue, ValueDifference<String>> entry : valueDiff.entrySet()) {
                BsonValue key = entry.getKey();
                result.addFailedKey(key);
            }
            result.addOnlyOnSourceKeys(diff.entriesOnlyOnLeft().keySet());
            result.addOnlyOnDestKeys(diff.entriesOnlyOnRight().keySet());
            int numMatches = (int) (sourceDocs.size() - valueDiff.size()
                    - result.getOnlyOnSourceCount() - result.getOnlyOnDestCount());
            result.setMatches(numMatches);
        }
        result.setBytesProcessed(Math.max(sourceBytesProcessed.longValue(), destBytesProcessed.longValue()));
        long diffTime = System.currentTimeMillis() - compStart;
        logger.debug("[{}] computed diff in {} ms ({})",
                Thread.currentThread().getName(), diffTime, unitLogString());
        return result;
    }

    protected abstract MongoClient getLoadClient(Target target);

    protected Map<BsonValue, String> load(Collection<BsonValue> ids, Target target) {
        MongoClient loadClient = getLoadClient(target);
        LongAdder bytesProcessed = (target == SOURCE) ? sourceBytesProcessed : destBytesProcessed;

        Map<BsonValue, String> output = new HashMap<>();
        long loadStart = System.currentTimeMillis();
        MongoCollection<RawBsonDocument> coll = getRawCollection(loadClient, namespace.getNamespace());
//        Bson q = (ids != null && ids.size() > 0) ? formIdsQuery(ids) : query;

        FindIterable<RawBsonDocument> finder;

        if (ids != null && ids.size() > 0) {
            Bson q = formIdsQuery(ids);
            finder = coll.find(q).batchSize(10000);
        } else if (this instanceof PartitionDiffTask) {
            PartitionDiffTask pdt = (PartitionDiffTask) this;
            Bson q = pdt.getPartitionDiffQuery();
            finder = coll.find(q).batchSize(10000);
        } else {
            Pair<Bson, Bson> bounds = getChunkBounds();
            if (bounds == null) {
                finder = coll.find().batchSize(10000);
            } else {
                Bson min = bounds.getLeft();
                Bson max = bounds.getRight();
                Set<String> shardKeys = min.toBsonDocument().keySet();
                Document hintDoc = new Document();
                for (String sk : shardKeys) {
                    hintDoc.put(sk, 1);
                }
                finder = coll.find().min(min).max(max).hint(hintDoc).batchSize(10000);
            }
        }

        for (RawBsonDocument doc : finder) {
            BsonValue id = doc.get("_id");
            ByteBuf bb = doc.getByteBuffer();
            byte[] docBytes = bb.array();
            bytesProcessed.add(bb.remaining());

            String docHash = CodecUtils.md5Hex(docBytes);
            output.put(id, docHash);
        }
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("[{}] loaded {} {} docs for {} in {} ms ({})",
                Thread.currentThread().getName(), output.size(), target.getName(),
                namespace.getNamespace(), loadTime, unitLogString());
        return output;
    }

    protected void closeCursor(MongoCursor<RawBsonDocument> cursor) {
        try {
            if (cursor != null) {
                cursor.close();
            }
        } catch (Exception ignored) {
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

    public Namespace getNamespace() {
        return namespace;
    }
}