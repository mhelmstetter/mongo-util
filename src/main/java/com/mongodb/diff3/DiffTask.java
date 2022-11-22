package com.mongodb.diff3;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.model.Namespace;
import com.mongodb.util.CodecUtils;
import org.bson.BsonValue;
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
    protected Bson query;
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

    protected abstract Bson getDiffQuery();

    protected abstract String unitLogString();

    protected abstract DiffResult initDiffResult();

    protected abstract RetryTask createRetryTask(RetryStatus retryStatus, DiffResult result);
    protected abstract RetryTask endToken();


    @Override
    public DiffResult call() throws Exception {
        DiffResult result;
        start = System.currentTimeMillis();
        query = getDiffQuery();

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
        Bson q = (ids != null && ids.size() > 0) ? formIdsQuery(ids) : query;

        for (RawBsonDocument doc : coll.find(q).batchSize(10000)) {
            BsonValue id = doc.get("_id");
            byte[] docBytes = doc.getByteBuffer().array();
            bytesProcessed.add(docBytes.length);

            String docHash = CodecUtils.md5Hex(docBytes);
            output.put(id, docHash);
        }
        long loadTime = System.currentTimeMillis() - loadStart;
        logger.debug("[{}] loaded {} {} docs for {} in {} ms ({})",
                Thread.currentThread().getName(), output.size(), target.getName(), namespace.getNamespace(), loadTime, unitLogString());
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