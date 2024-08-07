package com.mongodb.diff3;

import static com.mongodb.diff3.DiffTask.Target.DEST;
import static com.mongodb.diff3.DiffTask.Target.SOURCE;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;

import com.mongodb.model.Database;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.IndexSpec;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.diff3.partition.PartitionDiffTask;
import com.mongodb.model.Namespace;
import com.mongodb.util.CodecUtils;

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
    //    protected Pair<Bson, Bson> bounds;
    protected ChunkDef chunkDef;
    protected long start;
    protected DiffSummary summary;

    protected long sourceBytesProcessed;
    protected long destBytesProcessed;

    protected MongoCursor<RawBsonDocument> sourceCursor = null;
    protected MongoCursor<RawBsonDocument> destCursor = null;

    protected Map<BsonValue, String> sourceDocs = null;
    protected Map<BsonValue, String> destDocs = null;

    public DiffTask(DiffConfiguration config, Namespace namespace, DiffSummary summary) {
        this.config = config;
        this.namespace = namespace;
        this.summary = summary;
    }

    //    protected abstract Bson getDiffQuery();
//    protected abstract Pair<Bson, Bson> getChunkBounds();

    protected abstract String unitString();


    //protected abstract RetryTask endToken();


    @Override
    public DiffResult call() throws Exception {
        DiffResult result;
        start = System.currentTimeMillis();

        try {
            result = computeDiff();
        } catch (Exception e) {
            logger.error("[{}] fatal error diffing ({})",
                    Thread.currentThread().getName(), unitString(), e);
            throw new RuntimeException(e);
        }

        long timeSpent = System.currentTimeMillis() - start;
        logger.debug("[{}] completed a diff task in {} ms ({})",
                Thread.currentThread().getName(), timeSpent, unitString());

        return result;
    }

    protected DiffResult computeDiff() {
        return computeDiff(null);
    }

    protected DiffResult computeDiff(Collection<BsonValue> ids) {

        for (int i = 1; i <= 3; i++) {
            try {
                sourceDocs = load(ids, SOURCE);
                destDocs = load(ids, DEST);
                break;
            } catch (MongoException me) {
                logger.warn("computeDiff caught mongo exception on attempt " + i, me);
            }
        }
        return doComparison();
    }

    private DiffResult doComparison() {
        long compStart = System.currentTimeMillis();
        MapDifference<BsonValue, String> diff = Maps.difference(sourceDocs, destDocs);

        Set<DiffResult.MismatchEntry> mismatches = new HashSet<>();
        Set<BsonValue> srcOnly = new HashSet<>();
        Set<BsonValue> destOnly = new HashSet<>();
        int numMatches;
        if (diff.areEqual()) {
            numMatches = sourceDocs.size();
//            result.setMatches(numMatches);
        } else {
            Map<BsonValue, ValueDifference<String>> valueDiff = diff.entriesDiffering();
            for (Map.Entry<BsonValue, ValueDifference<String>> entry : valueDiff.entrySet()) {
                BsonValue key = entry.getKey();
                ValueDifference<String> val = entry.getValue();
                mismatches.add(new DiffResult.MismatchEntry(key, val.leftValue(), val.rightValue()));
            }
            Set<BsonValue> onlyOnSource = diff.entriesOnlyOnLeft().keySet();
            if (!onlyOnSource.isEmpty()) {
                //logger.debug("[{}] {} - diff failure, onlyOnSource: {}", Thread.currentThread().getName(), namespace, onlyOnSource);
                srcOnly.addAll(onlyOnSource);
            }

            Set<BsonValue> onlyOnDest = diff.entriesOnlyOnRight().keySet();
            if (!onlyOnDest.isEmpty()) {
                //logger.warn("[{}] {} - diff failure, onlyOnDest: {}", Thread.currentThread().getName(), namespace, onlyOnDest);
                destOnly.addAll(onlyOnDest);
            }

            numMatches = (int) (sourceDocs.size() - valueDiff.size()
                    - onlyOnSource.size() - onlyOnDest.size());
//            result.setMatches(numMatches);
        }
        long bytes = Math.max(sourceBytesProcessed, destBytesProcessed);
        long diffTime = System.currentTimeMillis() - compStart;
        logger.trace("[{}] computed diff in {} ms ({})",
                Thread.currentThread().getName(), diffTime, unitString());
        return new DiffResult(numMatches, bytes, mismatches, srcOnly, destOnly, namespace, chunkDef);
    }

    protected abstract MongoClient getLoadClient(Target target);

    private BsonDocument findHintDoc(BsonDocument ref) {
        Set<String> shardKeys = ref.keySet();

        DatabaseCatalog dbCatalog = config.getSourceShardClient().getDatabaseCatalog();

        Database database = dbCatalog.getDatabase(namespace.getDatabaseName());
        com.mongodb.model.Collection coll = database.getCollection(namespace.getNamespace());
        Set<IndexSpec> collIndexes = coll.getIndexes();

        Set<IndexSpec> candidates = new HashSet<>();
        for (IndexSpec is : collIndexes) {
            boolean hasAllShardKeys = true;
            for (String sk : shardKeys) {
                if (!(is.getKey().containsKey(sk))) {
                    hasAllShardKeys = false;
                    break;
                }
            }
            if (hasAllShardKeys) {
                candidates.add(is);
            }
        }
        if (candidates.size() <= 0) {
            logger.error("Could not find an index that contains all the shard keys");
            throw new RuntimeException("Error creating hint doc for loading shard chunks");
        }
        return candidates.stream().min((i, j) -> {
            int iNumKeys = i.getKey().keySet().size();
            int jNumKeys = j.getKey().keySet().size();
            return iNumKeys - jNumKeys;
        }).get().getSourceSpec().toBsonDocument();
    }

    protected Map<BsonValue, String> load(Collection<BsonValue> ids, Target target) {
        MongoClient loadClient = getLoadClient(target);
        long bytesProcessed = 0;

        Map<BsonValue, String> output = new HashMap<>();
        long loadStart = System.currentTimeMillis();
        MongoCollection<RawBsonDocument> coll = getRawCollection(loadClient, namespace.getNamespace());
        FindIterable<RawBsonDocument> finder;

        if (ids != null && ids.size() > 0) {
            Bson q = formIdsQuery(ids);
            finder = coll.find(q).batchSize(10000);
        } else if (this instanceof PartitionDiffTask) {
            PartitionDiffTask pdt = (PartitionDiffTask) this;
            Bson q = pdt.getPartitionDiffQuery();
            finder = coll.find(q).batchSize(10000);
        } else {
//            Pair<Bson, Bson> bounds = getChunkBounds();
            // TODO: not sure if it's possible for one of min/max to be null and not the other
            if (chunkDef.getMin() == null) {
                finder = coll.find().batchSize(10000);
            } else {
                BsonDocument min = chunkDef.getMin();
                BsonDocument max = chunkDef.getMax();
                Set<String> shardKeys = min.keySet();
                BsonDocument hintDoc = findHintDoc(min);

                logger.debug("hint: {}", hintDoc);
                finder = coll.find().min(min).max(max).hint(hintDoc.getDocument("key")).batchSize(10000);
            }
        }

        for (RawBsonDocument doc : finder) {
            BsonValue id = doc.get("_id");
            ByteBuf bb = doc.getByteBuffer();
            byte[] docBytes = bb.array();
            bytesProcessed += bb.remaining();

            String docHash = CodecUtils.md5Hex(docBytes);
            output.put(id, docHash);
        }
        switch (target) {
            case SOURCE:
                sourceBytesProcessed = bytesProcessed;
                break;
            case DEST:
                destBytesProcessed = bytesProcessed;
                break;
            default:
                throw new RuntimeException("Unknown target");
        }
        if (logger.isTraceEnabled()) {
            long loadTime = System.currentTimeMillis() - loadStart;
            logger.trace("[{}] loaded {} {} docs for {} in {} ms ({})",
                    Thread.currentThread().getName(), output.size(), target.getName(),
                    namespace.getNamespace(), loadTime, unitString());
        }

        return output;
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

    public ChunkDef getChunkDef() {
        return chunkDef;
    }
}