package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import com.mongodb.model.ShardCollection;
import com.mongodb.util.DiffUtils;
import com.mongodb.util.bson.BsonValueComparator;

public class DiffUtil {

	private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);

	private String sourceClusterUri;

	private String destClusterUri;

	private MongoClient sourceClient;
	private MongoClient destClient;

	private Set<Namespace> includedNamespaces = new HashSet<>();
    private Set<String> includedNamespaceStrings = new HashSet<>();
    private Set<String> includedDatabases = new HashSet<>();
    private Map<String, Set<String>> includedCollections = new HashMap<>();

	private final static Document SORT_ID = new Document("_id", 1);

	private Map<String, Document> sourceDbInfoMap = new TreeMap<String, Document>();
	private Map<String, Document> destDbInfoMap = new TreeMap<String, Document>();

	private BsonValueComparator comparator = new BsonValueComparator();

	MongoDatabase currentSourceDb;
	MongoDatabase currentDestDb;

	private String currentDbName;
	private String currentCollectionName;
	private String currentNs;

	long totalDbs = 0;
	long missingDbs = 0;
	long totalCollections = 0;
	long totalMatches = 0;
	long totalMissingDocs = 0;
	long totalKeysMisordered = 0;
	long totalHashMismatched = 0;

	@SuppressWarnings("unchecked")
	public void init() {

		MongoClientURI source = new MongoClientURI(sourceClusterUri);

		sourceClient = new MongoClient(source);
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

		if (includedNamespaces.isEmpty()) {
			populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
		} else {
			for (Document dbInfo : sourceDatabaseInfo) {
				String dbName = dbInfo.getString("name");
				if (includedDatabases.contains(dbName)) {
					sourceDbInfoMap.put(dbName, dbInfo);
				}
				
			}
		}
		
		populateDbMap(destDatabaseInfo, destDbInfoMap);
	}

	public void compareDocuments() {
		logger.debug("Starting compareDocuments mode");
		
		boolean filtered = !includedCollections.isEmpty();

		for (String dbName : sourceDbInfoMap.keySet()) {
			this.currentDbName = dbName;
			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {
				if (dbName.equals("admin") || dbName.equals("config") || dbName.equals("local")) {
					continue;
				}
				totalDbs++;
				currentSourceDb = sourceClient.getDatabase(dbName);
				currentDestDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = currentSourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					
					if (filtered) {
						Set<String> colls = includedCollections.get(dbName);
						if (! colls.contains(collectionName)) {
							continue;
						}
					}
					
					this.currentCollectionName = collectionName;
					this.currentNs = String.format("%s.%s", dbName, collectionName);
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					compareAllDocumentsInCollection();
				}
			} else {
				logger.error(String.format("database %s missing on destination or excluded via filter", dbName));
				missingDbs++;
			}
		}
		logger.debug(String.format(
				"%s dbs compared, %s collections compared, missingDbs %s, docMatches: %s, missingDocs: %s, hashMismatched: %s, keysMisordered: %s",
				totalDbs, totalCollections, missingDbs, totalMatches, totalMissingDocs, totalHashMismatched,
				totalKeysMisordered));

	}

	private void compareAllDocumentsInCollection() {

		long sourceCount = currentSourceDb.getCollection(currentCollectionName).countDocuments();
		long destCount = currentDestDb.getCollection(currentCollectionName).countDocuments();
		logger.debug(String.format("Starting collection: %s - %d documents", currentNs, sourceCount));
		if (sourceCount != destCount) {
			logger.error(String.format("%s - doc count mismatch - sourceCount: %s, destCount: %s", currentNs,
					sourceCount, destCount));
		}

		MongoCollection<RawBsonDocument> sourceColl = currentSourceDb.getCollection(currentCollectionName,
				RawBsonDocument.class);
		MongoCollection<RawBsonDocument> destColl = currentDestDb.getCollection(currentCollectionName,
				RawBsonDocument.class);

		MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(SORT_ID).iterator();
		MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(SORT_ID).iterator();

		RawBsonDocument sourceDoc = null;
		RawBsonDocument destDoc = null;
		byte[] sourceBytes = null;
		byte[] destBytes = null;
		long missing = 0;
		long matches = 0;
		long keysMisordered = 0;
		long hashMismatched = 0;
		long total = 0;
		long lastReport = System.currentTimeMillis();
		while (sourceCursor.hasNext()) {
			sourceDoc = sourceCursor.next();
			Object sourceId = sourceDoc.get("_id");
			if (destCursor.hasNext()) {
				destDoc = destCursor.next();
			} else {
				logger.error(String.format("%s - destCursor exhausted, doc %s missing", currentNs, sourceId));
				missing++;
				continue;
			}
			sourceBytes = sourceDoc.getByteBuffer().array();
			destBytes = destDoc.getByteBuffer().array();
			if (sourceBytes.length == destBytes.length) {
				if (!DiffUtils.compareHashes(sourceBytes, destBytes)) {
					Object id = sourceDoc.get("_id");

					if (sourceDoc.equals(destDoc)) {
						logger.error(String.format("%s - docs equal, but hash mismatch, id: %s", currentNs, id));
						keysMisordered++;
					} else {
						logger.error(String.format("%s - doc hash mismatch, id: %s", currentNs, id));
						hashMismatched++;
					}

				} else {
					matches++;
				}
			} else {
				logger.debug("Doc sizes not equal, id: " + sourceId);
				boolean xx = DiffUtils.compareDocuments(currentNs, sourceDoc, destDoc);
				hashMismatched++;
			}
			total++;
			long now = System.currentTimeMillis();
			long elapsedSinceLastReport = now - lastReport;
			if (elapsedSinceLastReport >= 30000) {
				logger.debug(String.format("%d percent complete", total / sourceCount));
				lastReport = now;
			}

		}
		totalCollections++;
		totalMatches += matches;
		totalKeysMisordered += keysMisordered;
		totalHashMismatched += hashMismatched;
		totalMissingDocs += missing;

		logger.debug(String.format("%s.%s complete - matches: %d, missing: %d, outOfOrderKeys: %s, hashMismatched: %d",
				currentSourceDb.getName(), currentCollectionName, matches, missing, keysMisordered, hashMismatched));

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
		logger.debug("Starting compareIds mode");
		Document sort = new Document("_id", 1);

		for (String dbName : sourceDbInfoMap.keySet()) {
			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {

				if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
					continue;
				}
				totalDbs++;

				MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
				MongoDatabase destDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					totalCollections++;
					logger.debug(String.format("Starting namespace %s.%s", dbName, collectionName));
					MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(collectionName,
							RawBsonDocument.class);
					MongoCollection<RawBsonDocument> destColl = destDb.getCollection(collectionName,
							RawBsonDocument.class);
					long sourceCount = 0;
					long destCount = 0;

					MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(sort).projection(sort)
							.iterator();
					MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(sort).projection(sort).iterator();

					RawBsonDocument sourceDoc = null;
					RawBsonDocument sourceNext = null;
					BsonValue sourceKey = null;

					RawBsonDocument destDoc = null;
					RawBsonDocument destNext = null;
					BsonValue destKey = null;
					Integer compare = null;

					while (sourceCursor.hasNext() || sourceNext != null || destCursor.hasNext() || destNext != null) {
						if (sourceNext != null) {
							sourceDoc = sourceNext;
							sourceNext = null;
							sourceKey = sourceDoc.get("_id");
						} else if (sourceCursor.hasNext()) {
							sourceDoc = sourceCursor.next();
							sourceCount++;
							sourceKey = sourceDoc.get("_id");
						} else {
							sourceDoc = null;
							sourceKey = null;
						}

						if (destNext != null) {
							destDoc = destNext;
							destNext = null;
							destKey = destDoc.get("_id");
						} else if (destCursor.hasNext()) {
							destDoc = destCursor.next();
							destCount++;
							destKey = destDoc.get("_id");
						} else {
							destDoc = null;
							destKey = null;
						}

						if (sourceKey != null && destKey != null) {
							compare = comparator.compare(sourceKey, destKey);
						} else if (sourceKey == null) {
							logger.error(String.format("%s - fail: %s missing on source", collectionName, destKey));
							totalMissingDocs++;
							continue;
						} else if (destKey == null) {
							logger.error(String.format("%s - fail: %s missing on dest", collectionName, sourceKey));
							totalMissingDocs++;
							continue;
						}

						if (compare < 0) {
							logger.error(String.format("%s - fail: %s missing on dest", collectionName, sourceKey));
							totalMissingDocs++;
							destNext = destDoc;
						} else if (compare > 0) {
							logger.warn(String.format("%s - fail: %s missing on source", collectionName, destKey));
							totalMissingDocs++;
							sourceNext = sourceDoc;
						} else {
							totalMatches++;
						}
					}

					logger.debug(String.format("%s - complete. sourceCount: %s, destCount: %s", collectionName,
							sourceCount, destCount));
				}

			} else {
				logger.error(String.format("Destination db not found, name: %s", dbName));
				missingDbs++;
			}
		}
		logger.debug(
				String.format("%s dbs compared, %s collections compared, missingDbs %s, idMatches: %s, missingDocs: %s",
						totalDbs, totalCollections, missingDbs, totalMatches, totalMissingDocs));
	}

	public void compareShardCounts() {
		// TODO Auto-generated method stub

	}

	public void setIncludes(String[] includes) {

		if (includes != null) {
			Collections.addAll(includedNamespaceStrings, includes);
			for (String nsStr : includes) {
				if (nsStr.contains(".")) {
					Namespace ns = new Namespace(nsStr);
					includedNamespaces.add(ns);
					includedDatabases.add(ns.getDatabaseName());
					Set<String> colls = includedCollections.get(ns.getDatabaseName());
					if (colls == null) {
						colls = new HashSet<>();
						includedCollections.put(ns.getDatabaseName(), colls);
					}
					colls.add(ns.getCollectionName());
				} else {
					includedDatabases.add(nsStr);
				}
			}
		}
	}

}
