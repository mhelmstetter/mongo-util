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
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.ShardCollection;
import com.mongodb.util.CodecUtils;
import com.mongodb.util.bson.BsonValueComparator;

public class DiffUtil {

	private static Logger logger = LoggerFactory.getLogger(DiffUtil.class);

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
		pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));

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

		populateDbMap(sourceDatabaseInfo, sourceDbInfoMap);
		populateDbMap(destDatabaseInfo, destDbInfoMap);
	}

	public void compareDocuments() {
		logger.debug("Starting compareDocuments mode");

		for (String dbName : sourceDbInfoMap.keySet()) {
			this.currentDbName = dbName;
			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {
				if (dbName.equals("admin") 
						|| dbName.equals("config")
						|| dbName.equals("local") ) {
					continue;
				}
				totalDbs++;
				currentSourceDb = sourceClient.getDatabase(dbName);
				currentDestDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = currentSourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					this.currentCollectionName = collectionName;
					this.currentNs = String.format("%s.%s", dbName, collectionName);
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					compareAllDocumentsInCollection();
				}
			} else {
				logger.error(String.format("database %s missing on destination", dbName));
				missingDbs++;
			}
		}
		logger.debug(String.format("%s dbs compared, %s collections compared, missingDbs %s, docMatches: %s, missingDocs: %s, hashMismatched: %s, keysMisordered: %s",
				totalDbs, totalCollections, missingDbs, totalMatches, totalMissingDocs, totalHashMismatched, totalKeysMisordered));

	}

	private void compareAllDocumentsInCollection() {
		
		long sourceCount = currentSourceDb.getCollection(currentCollectionName).countDocuments();
		long destCount = currentDestDb.getCollection(currentCollectionName).countDocuments();
		logger.debug(String.format("Starting collection: %s - %d documents", currentNs,
				sourceCount));
		if (sourceCount != destCount) {
			logger.error(String.format("%s - doc count mismatch - sourceCount: %s, destCount: %s", 
					currentNs, sourceCount, destCount));
		}

		MongoCollection<RawBsonDocument> sourceColl = currentSourceDb.getCollection(currentCollectionName, RawBsonDocument.class);
		MongoCollection<RawBsonDocument> destColl = currentDestDb.getCollection(currentCollectionName, RawBsonDocument.class);

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
				if (!compareHashes(sourceBytes, destBytes)) {
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
				boolean xx = compareDocuments(sourceDoc, destDoc);
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

		logger.debug(String.format("%s.%s complete - matches: %d, missing: %d, outOfOrderKeys: %s, hashMismatched: %d", currentSourceDb.getName(),
				currentCollectionName, matches, missing, keysMisordered, hashMismatched));

	}

	/**
	 * This comparison handles the (very) special case that we could have (usually
	 * due to some client/driver bug) 2 documents that differ only by the order of
	 * their fields.
	 * 
	 * @param sourceDoc
	 * @param destDoc
	 * @return
	 */
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
				logger.debug(String.format("    - values not equal for key: %s, sourceVal: %s, destVal: %s", key,
						sourceVal, destVal));
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
				logger.debug(String.format("%s.%s - bytes match: %s", currentDbName, currentCollectionName, newDocsMatch));
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
					
					logger.debug(String.format("%s - complete. sourceCount: %s, destCount: %s", collectionName, sourceCount, destCount));
				}

			} else {
				logger.error(String.format("Destination db not found, name: %s", dbName));
				missingDbs++;
			}
		}
		logger.debug(String.format("%s dbs compared, %s collections compared, missingDbs %s, idMatches: %s, missingDocs: %s",
				totalDbs, totalCollections, missingDbs, totalMatches, totalMissingDocs));
	}

}
