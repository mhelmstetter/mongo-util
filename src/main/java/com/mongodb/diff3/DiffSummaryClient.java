package com.mongodb.diff3;

import static com.mongodb.client.model.Projections.exclude;

import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.diff3.DiffSummary.DiffStatus;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class DiffSummaryClient {
    private final MongoClient client;
    private final MongoDatabase db;
    private final MongoCollection<RawBsonDocument> coll;
    private final MongoCollection<BsonDocument> simpleColl;
    private static final Logger logger = LoggerFactory.getLogger(DiffSummaryClient.class);


    public DiffSummaryClient(String uri, String dbName, String collName) {
        ConnectionString connStr = new ConnectionString(uri);
        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(connStr)
                .uuidRepresentation(UuidRepresentation.STANDARD).build();
        this.client = MongoClients.create(mcs);
        this.db = this.client.getDatabase(dbName);
        this.coll = this.db.getCollection(collName, RawBsonDocument.class);
        this.simpleColl = this.db.getCollection(collName, BsonDocument.class);
        coll.createIndex(Indexes.compoundIndex(Indexes.ascending("ns"), Indexes.ascending("min"), Indexes.ascending("max")));
    }

    public Map<String, RawBsonDocument> loadChunksCache(BsonDocument chunkQuery) {

        Map<String, RawBsonDocument> chunksCache = new LinkedHashMap<>();
        //Bson projection = include("min", "max", "ns", "status");
        Bson projection = exclude("history");
        FindIterable<RawBsonDocument> sourceChunks = coll.find(chunkQuery).projection(projection).sort(Sorts.ascending("ns", "min"));

        for (Iterator<RawBsonDocument> sourceChunksIterator = sourceChunks.iterator(); sourceChunksIterator.hasNext(); ) {
            RawBsonDocument chunk = sourceChunksIterator.next();
            String chunkId = ShardClient.getIdFromChunk(chunk);
            chunksCache.put(chunkId, chunk);
        }
        return chunksCache;
    }

    public void simpleUpdate(ChunkDef cd, ChunkResult cr) {
        BsonValue min = cd.getMin() == null ? new BsonDocument() : cd.getMin();
        BsonValue max = cd.getMax() == null ? new BsonDocument() : cd.getMax();


        Bson filter = Filters.and(
                Filters.eq("ns", cd.getNs().getNamespace()),
                Filters.eq("min", min),
                Filters.eq("max", max)
        );

        BsonDocument res = coll.find(filter).first();

        Document newDoc = new Document();
        newDoc.put("ns", cd.getNs().getNamespace());
        newDoc.put("min", min);
        newDoc.put("max", max);
        newDoc.put("status", cr.getStatus().toString());
        newDoc.put("retryNum", cr.getRetryNum().intValue());
        newDoc.put("matches", cr.getMatches().longValue());
        newDoc.put("mismatches", cr.getMismatchDocs());
        newDoc.put("srcOnly", cr.getSourceOnly());
        newDoc.put("scrOnlyCount", cr.getSourceOnly().size());
        newDoc.put("destOnly", cr.getDestOnly());
        newDoc.put("destOnlyCount", cr.getDestOnly().size());
        newDoc.put("bytesProcessed", cr.getBytesProcessed().longValue());
        newDoc.put("timestamp", new Date());

        BsonDocument newBsonDoc = newDoc.toBsonDocument();
        if (res == null) {
            simpleColl.insertOne(newBsonDoc);
        } else {
            simpleColl.replaceOne(filter, newBsonDoc);
        }
    }

    public void update(ChunkDef cd, ChunkResult cr) {
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        opts.upsert(true);
        opts.returnDocument(ReturnDocument.AFTER);

        BsonValue min = cd.getMin() == null ? new BsonDocument() : cd.getMin();
        BsonValue max = cd.getMax() == null ? new BsonDocument() : cd.getMax();

        Bson filter = Filters.and(
                Filters.eq("ns", cd.getNs().getNamespace()),
                Filters.eq("min", min),
                Filters.eq("max", max)
        );

        List<Bson> updates = new ArrayList<>();

        // Aha SO IDIOMATIC!!
        Document historyDoc = new Document();
        List<Object> caArgs = new ArrayList<>();
        List<Object> ifNullArgs = new ArrayList<>();
        ifNullArgs.add("$history");
        ifNullArgs.add(new ArrayList<>());
        caArgs.add(new Document("$ifNull", ifNullArgs));
        List<Document> replList = new ArrayList<>();
        Document replDoc = new Document();
        replDoc.put("timestamp", "$timestamp");
        replDoc.put("status", "$status");
        replDoc.put("retryNum", "$retryNum");
        replDoc.put("matches", "$matches");
        replDoc.put("mismatches", "$mismatches");
        replDoc.put("srcOnly", "$srcOnly");
        replDoc.put("destOnly", "$destOnly");
        replList.add(replDoc);
        caArgs.add(replList);
        historyDoc.put("$concatArrays", caArgs);

        if (cr.getRetryNum().intValue() > 1) {
            updates.add(Updates.set("history", historyDoc));
        }
        updates.add(Updates.set("status", cr.getStatus().toString()));
        updates.add(Updates.set("retryNum", cr.getRetryNum().intValue()));
        updates.add(Updates.set("matches", cr.getMatches().longValue()));
        updates.add(Updates.set("mismatches", cr.getMismatchDocs()));
        updates.add(Updates.set("mismatchesCount", cr.getMismatchDocs().size()));
        updates.add(Updates.set("srcOnly", cr.getSourceOnly()));
        updates.add(Updates.set("srcOnlyCount", cr.getSourceOnly().size()));
        updates.add(Updates.set("destOnly", cr.getDestOnly()));
        updates.add(Updates.set("destOnlyCount", cr.getDestOnly().size()));
        updates.add(Updates.set("bytesProcessed", cr.getBytesProcessed().longValue()));
        updates.add(Updates.set("timestamp", new Date()));

        logger.trace("Fire Status update Query");
        try {
            BsonDocument res = coll.findOneAndUpdate(filter, updates, opts);

            logger.trace("Status Update Result:: {}", res.toString());
        } catch (Exception e) {
//            logger.error("Update status query failed", e);
            logger.info("Update failed; trying simple update");
            throw new RuntimeException(e);
        }
    }

    public boolean updateChunkCompletion(Namespace ns, RawBsonDocument chunk, DiffSummary summary) {

        Bson filter = Filters.and(
                Filters.eq("ns", ns.getNamespace()),
                Filters.eq("min", chunk.get("min")),
                Filters.eq("max", chunk.get("max")),
                Filters.eq("status", DiffStatus.SUCCEEDED.toString())
        );


        long count = coll.countDocuments(filter);

//        RawBsonDocument aggResult = coll.aggregate(Arrays.asList(
//            Aggregates.match(filter),
//            Aggregates.project(Projections.fields(
//            		Projections.computed("mismatchCount", new Document("$size", "$mismatches")),
//            		Projections.computed("srcOnlyCount", new Document("$size", "$srcOnly")),
//            		Projections.computed("destOnlyCount", new Document("$size", "$destOnly"))
//            ))
//        )).first();
//        
//        if (aggResult != null) {
//        	return true;
//        }
//        return false;

        return count > 0;


    }
}
