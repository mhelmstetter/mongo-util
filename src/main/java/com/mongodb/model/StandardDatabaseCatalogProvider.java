package com.mongodb.model;

import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoIterable;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Collection;
import java.util.regex.Pattern;

public class StandardDatabaseCatalogProvider implements DatabaseCatalogProvider {
    private DatabaseCatalog databaseCatalog;
    private final MongoClient client;
    private Map<String, Document> collectionsMap;
    public final static Set<String> excludedSystemDbs =
            new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private static final Pattern excludeCollRegex = Pattern.compile("system\\..*");
    private static final Logger logger = LoggerFactory.getLogger(StandardDatabaseCatalogProvider.class);

    public StandardDatabaseCatalogProvider(MongoClient client){
        this.client = client;
    }

    @Override
    public DatabaseCatalog get() {
        return get(null);
    }

    @Override
    public DatabaseCatalog get(Collection<Namespace> namespaces) {
        if (databaseCatalog == null) {
            databaseCatalog = new DatabaseCatalog();
            populateDatabaseCatalog(namespaces);
        }
        return databaseCatalog;
    }
    
    @Override
    public void populateDatabaseCatalog() {
    	populateDatabaseCatalog(null);
    }

    private void populateDatabaseCatalog(Collection<Namespace> namespaces) {
    	databaseCatalog = new DatabaseCatalog();
        MongoIterable<String> dbNames = client.listDatabaseNames();
        Map<String, Set<String>> includeMap = new HashMap<>();
        boolean includeAll = true;
        if (namespaces != null && namespaces.size() > 0) {
            includeAll = false;
            namespaces.forEach(n -> {
                String db = n.getDatabaseName();
                String coll = n.getCollectionName();
                if (!includeMap.containsKey(db)) {
                    includeMap.put(db, new HashSet<>());
                }
                includeMap.get(db).add(coll);
            });
        }
        for (String dbName : dbNames) {
            if (excludedSystemDbs.contains(dbName) || (!includeAll && !includeMap.containsKey(dbName))) {
                logger.debug("Excluding db: {}", dbName);
                continue;
            }
            Database db = new Database(dbName);
            ListCollectionsIterable<Document> colls = listCollections(dbName);
            for (Document coll : colls) {
                String collName = coll.getString("name");

                if (!includeAll && !includeMap.get(dbName).contains(collName)) {
                    logger.debug("Excluding coll: {} in db: {}", collName, dbName);
                    continue;
                }

                String collType = coll.getString("type");
                if (collType.equals("view")) {
                    logger.info("Excluding view: {}", collName);
                    db.excludeCollection(collName);
                    continue;
                }
                /* Don't include collections starting with system.* */
                if (excludeCollRegex.matcher(collName).matches()) {
                    logger.debug("Excluding collection: {}", collName);
                    db.excludeCollection(collName);
                    continue;
                }
                Namespace collNs = new Namespace(dbName, collName);
                CollectionStats collStats = CollectionStats.fromDocument(collStats(collNs));
                boolean sharded = collectionsMap != null && collectionsMap.containsKey(collNs.getNamespace());
                com.mongodb.model.Collection mcoll = new com.mongodb.model.Collection(collNs, sharded, collStats);

                String shardedStatus = sharded ? "sharded" : "unsharded";
                db.addCollection(mcoll);
                logger.debug("Added {} collection {} to catalog for db {}, stats: {}", shardedStatus, collNs, dbName, collStats);
            }
            logger.debug("Add database {} to catalog with {} docs", dbName, db.getTotalDocumentCount());
            databaseCatalog.addDatabase(db);
        }
    }

    private Document dbStats(String dbName) {
        return client.getDatabase(dbName).runCommand(new Document("dbStats", 1));
    }

    private ListCollectionsIterable<Document> listCollections(String dbName) {
        return client.getDatabase(dbName).listCollections();
    }

    private Document collStats(Namespace ns) {
        return client.getDatabase(ns.getDatabaseName()).runCommand(new Document("collStats", ns.getCollectionName()));
    }

    public Map<String, Document> getCollectionsMap() {
        return collectionsMap;
    }

    public void setCollectionsMap(Map<String, Document> collectionsMap) {
        this.collectionsMap = collectionsMap;
    }
}
