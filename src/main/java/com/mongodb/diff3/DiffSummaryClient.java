package com.mongodb.diff3;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.PushOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.bson.json.JsonWriterSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DiffSummaryClient {
    private final MongoClient client;
    private final MongoDatabase db;
    private final MongoCollection<BsonDocument> coll;
    private static final Logger logger = LoggerFactory.getLogger(DiffSummaryClient.class);


    public DiffSummaryClient(String uri, String dbName, String collName) {
        ConnectionString connStr = new ConnectionString(uri);
        MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(connStr)
                .uuidRepresentation(UuidRepresentation.STANDARD).build();
        this.client = MongoClients.create(mcs);
        this.db = this.client.getDatabase(dbName);
        this.coll = this.db.getCollection(collName, BsonDocument.class);
    }

    public void update(ChunkDef cd, DiffSummary.ChunkResult cr) {
        FindOneAndUpdateOptions opts = new FindOneAndUpdateOptions();
        opts.upsert(true);
        opts.returnDocument(ReturnDocument.AFTER);

        BsonDocument chunkBounds = new BsonDocument();
        chunkBounds.put("min", cd.getMin() == null ? new BsonString("ALL") : cd.getMin());
        chunkBounds.put("max", cd.getMax() == null ? new BsonString("ALL") : cd.getMax());

        Bson filter = Filters.and(
                Filters.eq("db", cd.getNs().getDatabaseName()),
                Filters.eq("coll", cd.getNs().getCollectionName()),
                Filters.eq("chunk", chunkBounds)
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
        updates.add(Updates.set("srcOnly", cr.getSourceOnly()));
        updates.add(Updates.set("destOnly", cr.getDestOnly()));
        updates.add(Updates.set("bytesProcessed", cr.getBytesProcessed().longValue()));
        updates.add(Updates.set("timestamp", new Date()));

        logger.trace("Fire Status update Query");
        try {
            BsonDocument res = coll.findOneAndUpdate(filter, updates, opts);
            logger.trace("Status Update Result:: {}", res.toString());
        } catch (Exception e) {
            logger.error("Update status query failed", e);
            throw new RuntimeException(e);
        }
    }
}
