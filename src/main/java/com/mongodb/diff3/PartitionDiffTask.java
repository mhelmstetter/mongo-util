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



public class PartitionDiffTask implements Callable<PartitionDiffResult> {
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

    private final Partition partition;
    private final MongoClient sourceClient;
    private final MongoClient destClient;
    private static final Logger logger = LoggerFactory.getLogger(PartitionDiffTask.class);
    private long start;
    static final PartitionDiffTask END_TOKEN = new PartitionDiffTask(null, null, null, null);

    protected LongAdder sourceBytesProcessed = new LongAdder();
    protected LongAdder destBytesProcessed = new LongAdder();

    protected MongoCursor<RawBsonDocument> sourceCursor = null;
    protected MongoCursor<RawBsonDocument> destCursor = null;

    protected Map<BsonValue, String> sourceDocs = null;
    protected Map<BsonValue, String> destDocs = null;
    protected Queue<PartitionRetryTask> retryQueue;
    private Bson query;


    public PartitionDiffTask(Partition partition, MongoClient sourceClient,
                             MongoClient destClient, Queue<PartitionRetryTask> retryQueue) {
        this.partition = partition;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.retryQueue = retryQueue;
    }

    @Override
    public PartitionDiffResult call() throws Exception {
        PartitionDiffResult result;
        start = System.currentTimeMillis();
        query = new BsonDocument();
        if (partition != null) {
            query = partition.query();
        }

        try {
            result = computeDiff();
        } catch (Exception e) {
            logger.error("[{}] fatal error diffing ({})", Thread.currentThread().getName(), partition.toString(), e);
            throw new RuntimeException(e);
        } finally {
            closeCursor(sourceCursor);
            closeCursor(destCursor);
        }

        result.setPartition(partition);
        result.setNamespace(partition.getNamespace());

        if (result.getFailureCount() > 0) {
            RetryStatus retryStatus = new RetryStatus(0, System.currentTimeMillis());
            PartitionRetryTask retryTask = new PartitionRetryTask(retryStatus, this, result, result.getFailedIds(), retryQueue);
            retryQueue.add(retryTask);
            logger.debug("[{}] detected {} failures and added a retry task ({})",
                    Thread.currentThread().getName(), result.getFailureCount(), partition.toString());
        } else {
            retryQueue.add(PartitionRetryTask.END_TOKEN);
        }
        long timeSpent = System.currentTimeMillis() - start;
        logger.debug("[{}] completed a task in {} ms :: {}",
                Thread.currentThread().getName(), timeSpent, result.shortString());
        return result;
    }

    protected PartitionDiffResult computeDiff() {
        return computeDiff(null);
    }

    protected PartitionDiffResult computeDiff(Collection<BsonValue> ids) {
        sourceDocs = load(ids, Target.SOURCE);
        destDocs = load(ids, Target.DEST);
        return doComparison();
    }

    private PartitionDiffResult doComparison() {
        PartitionDiffResult result = new PartitionDiffResult();
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
        logger.debug("[{}] computed diff in {} ms ({})",
                Thread.currentThread().getName(), diffTime, partition.toString());
        return result;
    }

    protected Map<BsonValue, String> load(Collection<BsonValue> ids, Target target) {
        Map<BsonValue, String> output = new HashMap<>();
        long loadStart = System.currentTimeMillis();
        MongoClient client;

        LongAdder bytesProcessed;
        switch (target) {
            case SOURCE:
                client = sourceClient;
                bytesProcessed = sourceBytesProcessed;
                break;
            case DEST:
                client = destClient;
                bytesProcessed = destBytesProcessed;
                break;
            default:
                throw new RuntimeException("Unexpected target type: " + target.getName());
        }
        MongoCollection<RawBsonDocument> coll = getRawCollection(client, partition.getNamespace().getNamespace());
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
        logger.debug("[{}] loaded {} {} docs in {} ms ({})",
                Thread.currentThread().getName(), output.size(), target.getName(), loadTime, partition.toString());
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

    public Partition getPartition() {
        return partition;
    }
}
