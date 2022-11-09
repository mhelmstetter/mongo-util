package com.mongodb.diff3;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.model.Namespace;
import com.mongodb.util.CodecUtils;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;


public class PartitionDiffTask implements Callable<PartitionDiffResult> {
    private final Namespace namespace;
    private final MongoClient sourceClient;
    private final MongoClient destClient;
    private final PartitionManager partitionManager;
    private static final Logger logger = LoggerFactory.getLogger(PartitionDiffTask.class);
    private long start;


    public PartitionDiffTask(Namespace namespace, MongoClient sourceClient, MongoClient destClient,
                             PartitionManager partitionManager) {
        this.namespace = namespace;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.partitionManager = partitionManager;
    }

    @Override
    public PartitionDiffResult call() throws Exception {
        start = System.currentTimeMillis();
        PartitionDiffResult result = new PartitionDiffResult();
        logger.debug("Thread [{}] got a task for {}", Thread.currentThread().getName(), namespace.getNamespace());

        result.namespace = namespace;
        List<Partition> partitions = partitionManager.partitionCollection(namespace, sourceClient);
        logger.debug("Thread [{}] created {} partitions for {}",
                Thread.currentThread().getName(), partitions.size(), namespace);

        Collections.shuffle(partitions);
        for (Partition p : partitions) {
            logger.debug("Thread [{}] is processing partition: {}", Thread.currentThread().getName(), p.toString());

            PartitionDiffResult subResult = fetchAndCompare(p);
            result.add(subResult);
        }
        long timeSpent = timeSpent(System.currentTimeMillis());
        logger.debug("Thread [{}] completed a task in {} ms :: {}",
                Thread.currentThread().getName(), timeSpent, result.shortString());
        return result;
    }

    private PartitionDiffResult fetchAndCompare(Partition p) {
        logger.debug("Thread [{}] started fetch/compare for {}", Thread.currentThread().getName(), p.toString());
        PartitionDiffResult result = new PartitionDiffResult();
        result.namespace = namespace;
        result.partition = p;
        LongAdder srcBytes = new LongAdder();
        LongAdder destBytes = new LongAdder();
        Map<String, String> sourceDocs = fetchSourceDocs(p, srcBytes);
        Map<String, String> destDocs = fetchDestDocs(p, destBytes);

        long compStart = System.currentTimeMillis();
        MapDifference<String, String> diff = Maps.difference(sourceDocs, destDocs);

        if (diff.areEqual()) {
            result.matches = sourceDocs.size();
        } else {
            Map<String, ValueDifference<String>> vdiff = diff.entriesDiffering();
            result.matches = sourceDocs.size() - vdiff.size();
            for (Iterator<?> it = vdiff.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, ValueDifference<String>> entry =
                        (Map.Entry<String, ValueDifference<String>>) it.next();
                String key = entry.getKey();
                result.addFailedKey(key);
            }
            result.onlyOnSource = diff.entriesOnlyOnLeft().size();
            result.onlyOnDest = diff.entriesOnlyOnRight().size();
        }
        result.bytesProcessed = Math.max(srcBytes.longValue(), destBytes.longValue());
        long diffTime = System.currentTimeMillis() - compStart;
        logger.debug("Thread [{}] computed diff for {} in {} ms",
                Thread.currentThread().getName(), p, diffTime);

        return result;
    }

    private Map<String, String> fetchDocs(MongoClient client, Partition p, LongAdder numBytes) {
        int cap = (int) ((p.getEstimatedDocCount() * 4) / 3);
        Map<String, String> output = new HashMap<>(cap);
        String dbName = p.getNamespace().getDatabaseName();
        String collName = p.getNamespace().getCollectionName();
        MongoCollection<RawBsonDocument> coll = client.getDatabase(dbName)
                .getCollection(collName, RawBsonDocument.class);
        Bson pquery = p.query();
        MongoCursor<RawBsonDocument> cursor = coll.find(pquery).iterator();
        try {
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                String id = doc.get("_id").toString();
                byte[] docBytes = doc.getByteBuffer().array();
                numBytes.add(docBytes.length);

                String docHash = CodecUtils.md5Hex(docBytes);
                output.put(id, docHash);
            }
        } catch (Exception e) {
            logger.error("Thread [{}] encountered a fatal error fetching docs for {}",
                    Thread.currentThread().getName(), p, e);
            output.clear();
        } finally {
            try {
                if (cursor != null) {
                    cursor.close();
                }
            } catch (Exception e) {

            }
        }
        return output;
    }

    private Map<String, String> fetchSourceDocs(Partition p, LongAdder numBytes) {
        long fetchStart = System.currentTimeMillis();
        logger.debug("Thread [{}] started fetching docs for {} from source",
                Thread.currentThread().getName(), p);
        Map<String, String> output = fetchDocs(sourceClient, p, numBytes);
        long fetchTime = System.currentTimeMillis() - fetchStart;
        logger.debug("Thread [{}] fetched {} docs from source for {} in {} ms",
                Thread.currentThread().getName(), output.size(), p, fetchTime);
        return output;
    }

    private Map<String, String> fetchDestDocs(Partition p, LongAdder numBytes) {
        long fetchStart = System.currentTimeMillis();
        logger.debug("Thread [{}] started fetching docs for {} from dest",
                Thread.currentThread().getName(), p);
        Map<String, String> output = fetchDocs(destClient, p, numBytes);
        long fetchTime = System.currentTimeMillis() - fetchStart;
        logger.debug("Thread [{}] fetched {} docs from dest for {} in {} ms",
                Thread.currentThread().getName(), output.size(), p, fetchTime);
        return output;
    }

    protected long timeSpent(long stop) {
        return stop - start;
    }
}
