package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.eq;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.ShardCollection;
import com.mongodb.util.CodecUtils;

// TODO - look at https://github.com/10gen/scripts-and-snippets/blob/master/mongod/recreate-splits.js
public class DiffUtil {

    private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);

    private static final String MONGODB_SRV_PREFIX = "mongodb+srv://";
    private final static Document LOCALE_SIMPLE = new Document("locale", "simple");

    private String sourceClusterUri;

    private String destClusterUri;

    private MongoClient sourceClient;
    private MongoClient destClient;

    private final static Document SORT_ID = new Document("_id", 1);

    private final Codec<RawBsonDocument> rawCodec = new RawBsonDocumentCodec();

    CodecRegistry pojoCodecRegistry;

    private List<ShardCollection> sourceCollections = new ArrayList<ShardCollection>();

    private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
    private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();

    public DiffUtil() {
        logger.debug("DiffUtil starting");
    }

    public void init() {
        pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        MongoClientURI source = new MongoClientURI(sourceClusterUri);

        sourceClient = new MongoClient(source);
        List<ServerAddress> addrs = sourceClient.getServerAddressList();
        sourceClient.getDatabase("admin").runCommand(new Document("ping", 1));
        logger.debug("Connected to source");

        MongoClientURI dest = new MongoClientURI(destClusterUri);

        destClient = new MongoClient(dest);
        destClient.getDatabase("admin").runCommand(new Document("ping", 1));

        Document listDatabases = new Document("listDatabases", 1);
        Document sourceDatabases = sourceClient.getDatabase("admin").runCommand(listDatabases);
        Document destDatabases = destClient.getDatabase("admin").runCommand(listDatabases);

        List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
        List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

        populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
        populateDbMap(destDatabaseInfo, destDbInfoMap);
    }

    // @SuppressWarnings("unchecked")
    // public void compareShardCounts() {
    //
    // logger.debug("Starting compareShardCounts mode");
    //
    // for (String dbName : sourceDbInfoMap.keySet()) {
    // Document destInfo = destDbInfoMap.get(dbName);
    // if (destInfo != null) {
    // logger.debug(String.format("Found matching database %s", dbName));
    //
    // MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
    // MongoDatabase destDb = destClient.getDatabase(dbName);
    // MongoIterable<String> sourceCollectionNames =
    // sourceDb.listCollectionNames();
    // for (String collectionName : sourceCollectionNames) {
    // if (dbName.equals("admin") || dbName.equals("local") ||
    // collectionName.equals("system.profile")) {
    // continue;
    // }
    //
    // long sourceCount = sourceDb.getCollection(collectionName).count();
    // long destCount = destDb.getCollection(collectionName).count();
    // if (sourceCount == destCount) {
    // logger.debug(String.format("%s.%s count matches: %s", dbName,
    // collectionName, sourceCount));
    // } else {
    // logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s",
    // dbName, collectionName,
    // sourceCount, destCount));
    // }
    // compareChunks(sourceDb, destDb, collectionName);
    // }
    // } else {
    // logger.warn(String.format("Destination db not found, name: %s", dbName));
    // }
    // }
    // }

    public void compareChunks() {
        logger.debug("Starting chunkCounts mode");

        for (String dbName : sourceDbInfoMap.keySet()) {
            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo != null) {
                logger.debug(String.format("Found matching database %s", dbName));

                MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
                MongoDatabase destDb = destClient.getDatabase(dbName);
                MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
                for (String collectionName : sourceCollectionNames) {
                    if (dbName.equals("admin") || dbName.equals("local") || collectionName.equals("system.profile")) {
                        continue;
                    }
                    hashChunk(sourceDb, destDb, collectionName);
                }
            }

        }

    }

    // public void compareChunks(MongoDatabase sourceDb, MongoDatabase destDb,
    // String collectionName) {
    // long sourceTotal = 0;
    // long destTotal = 0;
    // String dbName = sourceDb.getName();
    // String ns = dbName + "." + collectionName;
    // List<Document> splitPoints = splitVector(ns);
    // if (splitPoints.isEmpty()) {
    // return;
    // }
    // Document lastSplitPoint = null;
    // Document query = null;
    // for (Document splitPoint : splitPoints) {
    //
    // if (lastSplitPoint == null) {
    // query = new Document("_id", new Document("$lte", splitPoint));
    // } else {
    // Document rhs = new Document("$gt", lastSplitPoint.get("_id"));
    // rhs.append("$lte", splitPoint.get("_id"));
    // query = new Document("_id", rhs);
    // }
    // long[] counts = doCount(sourceDb, destDb, collectionName, query);
    // hashChunk(sourceDb, destDb, collectionName, query);
    // sourceTotal += counts[0];
    // destTotal += counts[1];
    // lastSplitPoint = splitPoint;
    // }
    // // last chunk
    // query = new Document("_id", new Document("$gte",
    // lastSplitPoint.get("_id")));
    // long[] counts = doCount(sourceDb, destDb, collectionName, query);
    // sourceTotal += counts[0];
    // destTotal += counts[1];
    //
    // logger.debug(String.format("%s.%s sourceTotal: %s, destTotal: %s",
    // dbName, collectionName, sourceTotal, destTotal));
    // }

    private long[] doCount(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName, Document query) {
        logger.debug("auery: " + query);
        String dbName = sourceDb.getName();
        long sourceCount = sourceDb.getCollection(collectionName).count(query);
        long destCount = destDb.getCollection(collectionName).count(query);
        if (sourceCount == destCount) {
            logger.debug(String.format("%s.%s chunk count matches: %s", dbName, collectionName, sourceCount));
        } else {
            logger.warn(String.format("%s.%s chunk count MISMATCH - source: %s, dest: %s", dbName, collectionName,
                    sourceCount, destCount));
        }
        return new long[] { sourceCount, destCount };
    }

    private void hashChunk(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
        logger.debug(String.format("Starting collection: %s.%s", sourceDb.getName(), collectionName));
        MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(collectionName, RawBsonDocument.class);
        MongoCollection<RawBsonDocument> destColl = destDb.getCollection(collectionName, RawBsonDocument.class);

        MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(SORT_ID).iterator();
        MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(SORT_ID).iterator();

        RawBsonDocument sourceDoc = null;
        RawBsonDocument destDoc = null;
        byte[] sourceBytes = null;
        byte[] destBytes = null;
        long matches = 0;
        long failures = 0;
        while (sourceCursor.hasNext()) {
            sourceDoc = sourceCursor.next();
            if (destCursor.hasNext()) {
                destDoc = destCursor.next();
            } else {
                logger.error("counts don't match!!!!!");
                break;
            }
            sourceBytes = sourceDoc.getByteBuffer().array();
            destBytes = destDoc.getByteBuffer().array();
            if (sourceBytes.length == destBytes.length) {
                if (!compareHashes(sourceBytes, destBytes)) {
                    Object id = sourceDoc.get("_id");

                    if (sourceDoc.equals(destDoc)) {
                        logger.debug("Docs equal but hashes don't match, id: " + id);
                        matches++;
                    } else {
                        logger.debug("Hashes and docs don't match, id: " + id);
                        failures++;
                    }

                } else {
                    matches++;
                }
            } else {
                Object id = sourceDoc.get("_id");
                logger.debug("Doc sizes not equal, id: " + id);
                boolean xx = compareDocuments(sourceDoc, destDoc);
                failures++;
            }

        }

        logger.debug(String.format("%s.%s complete matches: %d, failures: %d", sourceDb.getName(), collectionName,
                matches, failures));

    }

    private boolean compareDocuments(RawBsonDocument sourceDoc, RawBsonDocument destDoc) {
        Object id = sourceDoc.get("_id");
        Set<String> sourceKeys = sourceDoc.keySet();
        Set<String> destKeys = destDoc.keySet();
        Set<String> sortedKeys = new TreeSet<String>();
        boolean setsEqual = sourceKeys.equals(destKeys);
        Set<String> diff = null;
        if (!setsEqual) {
            diff = Sets.difference(sourceKeys, destKeys);
            logger.debug("    - keys do not match: keys missing from source" + diff);

            diff = Sets.difference(destKeys, sourceKeys);
            logger.debug("    - keys do not match: keys missing from dest" + diff);
            sortedKeys.addAll(diff);
        }

        sortedKeys.addAll(sourceKeys);

        BsonDocument sourceDocNew = new BsonDocument();
        BsonDocument destDocNew = new BsonDocument();
        for (String key : sortedKeys) {
            BsonValue sourceVal = sourceDoc.get(key);
            BsonValue destVal = destDoc.get(key);
            boolean valuesEqual = sourceVal != null && destVal != null && sourceVal.equals(destVal);
            if (!valuesEqual) {
                logger.debug(String.format("    - values not equal for key: %s, sourceVal: %s, destVal: %s", key, sourceVal,
                        destVal));
            }
            if (sourceVal != null) {
                sourceDocNew.append(key, sourceVal);
            }
            if (destVal != null) {
                destDocNew.append(key, destVal);
            }
            
            
            if (setsEqual) {
                RawBsonDocument sourceRawNew = new RawBsonDocument(sourceDocNew, new BsonDocumentCodec());
                RawBsonDocument destRawNew = new RawBsonDocument(destDocNew, new BsonDocumentCodec());
                boolean newDocsMatch = compareHashes(sourceRawNew.getByteBuffer().array(),
                        destRawNew.getByteBuffer().array());
                logger.debug("bytes match: " + newDocsMatch);
            }
        }
        return sourceDoc.equals(destDoc);
    }

    private static boolean compareHashes(byte[] sourceBytes, byte[] destBytes) {
        String sourceHash = CodecUtils.md5Hex(sourceBytes);
        String destHash = CodecUtils.md5Hex(destBytes);
        return sourceHash.equals(destHash);
    }

    private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
        for (Document dbInfo : dbInfoList) {
            databaseMap.put(dbInfo.getString("name"), dbInfo);
        }
    }

    private List<Document> splitVector(String namespace) {
        Document splitVectorCmd = new Document("splitVector", namespace);
        Document keyPattern = new Document("_id", 1);
        splitVectorCmd.append("keyPattern", keyPattern);
        splitVectorCmd.append("maxChunkSizeBytes", 1000000);
        Document splits = destClient.getDatabase("admin").runCommand(splitVectorCmd);
        List<Document> splitKeys = (List<Document>) splits.get("splitKeys");
        logger.debug("splits: " + splitKeys);
        return splitKeys;
    }

    private void populateCollectionList(MongoDatabase db, List<ShardCollection> list) {
        MongoCollection<ShardCollection> shardsColl = db.getCollection("collections", ShardCollection.class);
        shardsColl.find(eq("dropped", false)).sort(Sorts.ascending("_id")).into(list);
    }

    public String getSourceClusterUri() {
        return sourceClusterUri;
    }

    public void setSourceClusterUri(String sourceClusterUri) {
        this.sourceClusterUri = sourceClusterUri;
    }

    public String getDestClusterUri() {
        return destClusterUri;
    }

    public void setDestClusterUri(String destClusterUri) {
        this.destClusterUri = destClusterUri;
    }

    @SuppressWarnings("unchecked")
    public void compareIds() {
        logger.debug("*********** Starting compareIds mode");
        Document sort = new Document("_id", 1);

        for (String dbName : sourceDbInfoMap.keySet()) {
            Document destInfo = destDbInfoMap.get(dbName);
            if (destInfo != null) {
                logger.debug(String.format("Found matching database %s", dbName));

                MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
                MongoDatabase destDb = destClient.getDatabase(dbName);
                MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
                for (String collectionName : sourceCollectionNames) {
                    if (dbName.equals("admin") || dbName.equals("local") || collectionName.equals("system.profile")) {
                        continue;
                    }

                    MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(collectionName,
                            RawBsonDocument.class);
                    MongoCollection<RawBsonDocument> destColl = destDb.getCollection(collectionName,
                            RawBsonDocument.class);

                    MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(sort).projection(sort)
                            .iterator();
                    MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(sort).projection(sort).iterator();

                    RawBsonDocument sourceDoc = null;
                    RawBsonDocument destDoc = null;
                    while (sourceCursor.hasNext()) {
                        sourceDoc = sourceCursor.next();
                        if (destCursor.hasNext()) {
                            destDoc = destCursor.next();
                        } else {
                            logger.error(String.format("collection: %s counts don't match!!!!!", collectionName));
                        }
                        Comparable sourceKey = (Comparable) sourceDoc.get("_id");
                        Comparable destKey = (Comparable) destDoc.get("_id");

                        int compare = sourceKey.compareTo(destKey);
                        if (compare < 0) {
                            logger.warn(String.format("< 0 - key mismatch collection: %s, %s %s", collectionName,
                                    sourceKey, destKey));
                            sourceCursor.next();
                        } else if (compare > 0) {
                            logger.warn(String.format("> 0 - key mismatch collection: %s, %s %s", collectionName,
                                    sourceKey, destKey));
                        }

                        // if (! sourceKey.equals(destKey)) {
                        // logger.warn(String.format("key mismatch collection:
                        // %s, %s %s", collectionName, sourceKey, destKey));
                        // }
                    }

                }
            } else {
                logger.warn(String.format("Destination db not found, name: %s", dbName));
            }
        }
    }

}
