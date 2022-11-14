package com.mongodb.diff3;

import com.mongodb.ReadConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.max;
import static com.mongodb.client.model.Accumulators.sum;

public class PartitionManager {
    private final double sampleRate;
    private final int sampleMinDocs;
    private final int maxDocsToSamplePerPartition;
    private final long defaultPartitionSize;

    public PartitionManager(double sampleRate, int sampleMinDocs, int maxDocsToSamplePerPartition, long defaultPartitionSize) {
        this.sampleRate = sampleRate;
        this.sampleMinDocs = sampleMinDocs;
        this.maxDocsToSamplePerPartition = maxDocsToSamplePerPartition;
        this.defaultPartitionSize = defaultPartitionSize;
    }

    public List<Partition> partitionCollection(Namespace ns, MongoClient client) {
        List<Partition> output = new ArrayList<>();
        Pair<Long, Integer> collMetrics = getCollMetrics(client, ns);
        double collSize = collMetrics.getLeft();
        long collNumDocs = collMetrics.getRight();

        Object minIdBound = getOuterIdBound(ns, client, false);
        Object maxIdBound = getOuterIdBound(ns, client, true);

        int numPartitions = (int) ((collSize / defaultPartitionSize) + 1);
        List<Object> midIdBounds = getMidIdBounds(ns, client, numPartitions, collNumDocs);
        List<Object> allIdBounds = allIdBounds(minIdBound, midIdBounds, maxIdBound);
        long estimatedDocCount = collNumDocs / numPartitions;

        for (int i = 0; i < allIdBounds.size() - 1; i++) {
            output.add(new Partition(ns, allIdBounds.get(i), allIdBounds.get(i + 1), estimatedDocCount));
        }
        return output;
    }

    private List<Object> allIdBounds(Object min, List<Object> mid, Object max) {
        List<Object> output = new ArrayList<>();
        output.add(min);
        output.addAll(mid);
        output.add(max);
        return output;
    }

    private Pair<Long, Integer> getCollMetrics(MongoClient client, Namespace ns) {
        long collSize = 0;
        int collNumDocs = 0;
        String db = ns.getDatabaseName();
        String coll = ns.getCollectionName();

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(new Document("$collStats", new Document("storageStats", new Document("scale", 1))));
        pipeline.add(Aggregates.group("$ns", sum("count", "$storageStats.count"),
                sum("size", "$storageStats.size")));

        AggregateIterable<Document> results = client.getDatabase(db).getCollection(coll)
                .withReadConcern(ReadConcern.MAJORITY).aggregate(pipeline);

        MongoCursor<Document> cursor = results.iterator();
        Document first = cursor.next();
        collSize = asLong(first.get("size"));
        collNumDocs = asInt(first.get("count"));
        if (cursor.hasNext()) {
            throw new RuntimeException("Expected only result while getting collection metrics");
        }

        return Pair.of(collSize, collNumDocs);
    }

    private Object getOuterIdBound(Namespace ns, MongoClient client, boolean max) {
        String db = ns.getDatabaseName();
        String coll = ns.getCollectionName();

        List<Bson> pipeline = new ArrayList<>();
        Bson sortSpec = max ? Sorts.descending("_id") : Sorts.ascending("_id");
        pipeline.add(Aggregates.sort(Sorts.orderBy(sortSpec)));
        pipeline.add(Aggregates.project(Projections.fields(Projections.include("_id"))));
        pipeline.add(Aggregates.limit(1));

        AggregateIterable<Document> results = client.getDatabase(db).getCollection(coll)
                .withReadConcern(ReadConcern.MAJORITY)
                .aggregate(pipeline).hint(new Document("_id", 1));

        MongoCursor<Document> cursor = results.iterator();
        Document first = cursor.next();
        Object bound = first.get("_id");

        return bound;
    }

    private List<Object> getMidIdBounds(Namespace ns, MongoClient client, int numPartitions, long collDocCount) {
        if (numPartitions < 2 || sampleMinDocs > collDocCount) {
            return new ArrayList<>();
        }

        List<Object> output = new ArrayList<>(numPartitions);
        int numDocsToSample = (int) Math.min(sampleRate * collDocCount, maxDocsToSamplePerPartition * numPartitions);

        String db = ns.getDatabaseName();
        String coll = ns.getCollectionName();

        List<Bson> pipeline = new ArrayList<>();
        pipeline.add(Aggregates.sample(numDocsToSample));
        pipeline.add(Aggregates.project(Projections.fields(Projections.include("_id"))));
        pipeline.add(Aggregates.bucketAuto("$_id", numPartitions));

        AggregateIterable<Document> results = client.getDatabase(db).getCollection(coll)
                .aggregate(pipeline)
                .allowDiskUse(true);

        MongoCursor<Document> cursor = results.iterator();
        while (cursor.hasNext()) {
            Document doc = cursor.next();
            Object maxId = ((Document) doc.get("_id")).get("max");
            output.add(maxId);
        }

        return output;
    }

    private long asLong(Object o) {
        return ((Number) o).longValue();
    }

    private int asInt(Object o) {
        return ((Number) o).intValue();
    }
}
