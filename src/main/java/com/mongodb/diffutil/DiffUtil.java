package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
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
	
	private boolean reportMissing = true;
	private boolean reportMatches = false;
	
	private int batchSize = 200;
	
	private Double globalSampleRate;
	
	long sourceTotal = 0;
	long destTotal = 0;
	long lastReport;
	long sourceCount;
	long destCount;

	private Map<Namespace, Namespace> mappedNamespaces;
	
	private Properties configProperties;

	public DiffUtil(Properties configFileProps) {
		this.configProperties = configFileProps;
	}

	@SuppressWarnings("unchecked")
	public void init() {

		ConnectionString source = new ConnectionString(sourceClusterUri);

		sourceClient = MongoClients.create(source);
		sourceClient.getDatabase("admin").runCommand(new Document("ping", 1));
		logger.debug("Connected to source");

		ConnectionString dest = new ConnectionString(destClusterUri);

		destClient = MongoClients.create(dest);
		destClient.getDatabase("admin").runCommand(new Document("ping", 1));

		Document listDatabases = new Document("listDatabases", 1);
		Document sourceDatabases = sourceClient.getDatabase("admin").runCommand(listDatabases);
		Document destDatabases = destClient.getDatabase("admin").runCommand(listDatabases);

		List<Document> sourceDatabaseInfo = (List<Document>) sourceDatabases.get("databases");
		List<Document> destDatabaseInfo = (List<Document>) destDatabases.get("databases");

		if (includedNamespaces == null || includedNamespaces.isEmpty()) {
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

	public DiffSummary compareDocuments(boolean parallelCursorMode) {
		
		DiffSummary ds = null;
		boolean filtered = !includedCollections.isEmpty();
		
		logger.debug("Starting compareDocuments mode, filtered {}", filtered);

		if (parallelCursorMode) {
			ds = compare2();
		} else {
			ds = compareAllDocumentsInCollectionUsingQueryMode();
		}
		
		logger.debug(String.format(
				"%s dbs compared, %s collections compared, missingDbs %s, docMatches: %s, missingDocs: %s, hashMismatched: %s, keysMisordered: %s",
				ds.totalDbs, ds.totalCollections, ds.missingDbs, ds.totalMatches, ds.totalMissingDocs, ds.totalHashMismatched,
				ds.totalKeysMisordered));
		return ds;

	}
	
	private DiffSummary compareAllDocumentsInCollectionUsingQueryMode() {

		DiffSummary ds = new DiffSummary();
		Document sort = new Document("_id", 1);
		boolean filtered = !includedCollections.isEmpty();
		
		for (String dbName : sourceDbInfoMap.keySet()) {
			Document destInfo = destDbInfoMap.get(dbName);
			
			if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
				continue;
			}
			
			if (includedDatabases.size() > 0 && !includedDatabases.contains(dbName)) {
				continue;
			}
			
			if (destInfo != null) {

				ds.totalDbs++;

				MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
				MongoDatabase destDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					
					if (filtered) {
						Set<String> colls = includedCollections.get(dbName);
						if (! colls.contains(collectionName)) {
							continue;
						}
					}
					ds.totalCollections++;
					
					MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(collectionName,
							RawBsonDocument.class);
					MongoCollection<RawBsonDocument> destColl = destDb.getCollection(collectionName,
							RawBsonDocument.class);

					Double sampleRate = null;
					MongoCursor<RawBsonDocument> sourceCursor;
					
					String namespace = dbName + "." + collectionName;
					String sampleKey = namespace + "_sampleRate";
					if (configProperties.containsKey(sampleKey) || globalSampleRate != null) {
						String sampleRateStr = (String)configProperties.get(sampleKey);
						sampleRate = (sampleRateStr != null) ? Double.parseDouble(sampleRateStr) : globalSampleRate;
						long sCount = sourceDb.getCollection(collectionName).estimatedDocumentCount();
						int sBatch = Math.toIntExact(Math.round(sCount * sampleRate));
						logger.debug(String.format("Starting collection %s, namespace %s, sampleRate: %s, estDocCount: %,d, sampleBatchSize: %,d", 
								ds.totalCollections, namespace, sampleRate, sCount, sBatch));
						sourceCursor = sourceColl.aggregate(Arrays.asList(Aggregates.sample(sBatch))).iterator();
					} else {
						logger.debug(String.format("Starting collection %s, namespace %s", ds.totalCollections, namespace));
						sourceCursor = sourceColl.find().sort(sort).iterator();
					}
					
					RawBsonDocument sourceDoc = null;
					Map<BsonValue, RawBsonDocument> sourceBuffer = new TreeMap<>();
					Map<BsonValue, RawBsonDocument> destBuffer = new TreeMap<>();
					
					
					while (sourceCursor.hasNext()) {
						sourceDoc = sourceCursor.next();
						BsonValue sourceId = sourceDoc.get("_id");
						sourceBuffer.put(sourceId, sourceDoc);
						
						if (sourceBuffer.size() >= batchSize) {
							loadQueryBatch(sourceBuffer, destBuffer, destColl);
							compareDocBuffers(namespace, sourceBuffer, destBuffer, ds);
							sourceBuffer.clear();
							destBuffer.clear();
						}
					}
					loadQueryBatch(sourceBuffer, destBuffer, destColl);
					compareDocBuffers(namespace, sourceBuffer, destBuffer, ds);
				}
			}
		}
		

		logger.debug(String.format("complete - matches: %d, missing: %d, outOfOrderKeys: %s, hashMismatched: %d", 
				ds.totalMatches, ds.totalMissingDocs, ds.totalKeysMisordered, ds.totalHashMismatched));
		
		return ds;

	}
	
	private void loadQueryBatch(Map<BsonValue, RawBsonDocument> sourceBuffer, Map<BsonValue, RawBsonDocument> destBuffer,
			MongoCollection<RawBsonDocument> destColl) {
		MongoCursor<RawBsonDocument> dCursor = destColl.find(in("_id", sourceBuffer.keySet())).iterator();
		
		RawBsonDocument destDoc = null;
		while (dCursor.hasNext()) {
			destDoc = dCursor.next();
			BsonValue destId = destDoc.get("_id");
			destBuffer.put(destId, destDoc);
		}
	}


	private void compareDocBuffers(String ns, Map<BsonValue, RawBsonDocument> sourceBuffer,
			Map<BsonValue, RawBsonDocument> destBuffer, DiffSummary ds) {
		
		for (Map.Entry<BsonValue, RawBsonDocument> entry : sourceBuffer.entrySet()) {
			
			RawBsonDocument sourceDoc = entry.getValue();
			BsonValue sourceKey = entry.getKey();
			RawBsonDocument destDoc = destBuffer.get(sourceKey);
			
			if (destDoc == null) {
				if (reportMissing) {
					logger.error(String.format("%s - fail: %s missing on dest", ns, sourceKey));
				}
				ds.totalMissingDocs++;
			} else {
				DiffUtils.compare(ns, sourceDoc, sourceKey, destDoc, ds);
			}
			
			
		}
		
	}
	
	public void retry() {
		
	}

	private void populateDbMap(List<Document> dbInfoList, Map<String, Document> databaseMap) {
		for (Document dbInfo : dbInfoList) {
			databaseMap.put(dbInfo.getString("name"), dbInfo);
		}
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
	
	public DiffSummary compare2() {
		
		DiffSummary ds = new DiffSummary();
		
		Document sort = new Document("_id", 1);
		boolean filtered = !includedCollections.isEmpty();

		logger.debug("Starting compare2 mode, filtered: {}", filtered);
		
		for (String dbName : sourceDbInfoMap.keySet()) {
			Document destInfo = destDbInfoMap.get(dbName);
			
			if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
				continue;
			}
			
			if (includedDatabases.size() > 0 && !includedDatabases.contains(dbName)) {
				continue;
			}
 			
			if (destInfo != null) {

				ds.totalDbs++;

				MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
				MongoDatabase destDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					
					if (filtered) {
						Set<String> colls = includedCollections.get(dbName);
						if (! colls.contains(collectionName)) {
							continue;
						}
					}
					ds.totalCollections++;
					
					
					
					
					MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(collectionName,
							RawBsonDocument.class);
					MongoCollection<RawBsonDocument> destColl = destDb.getCollection(collectionName,
							RawBsonDocument.class);
					long sourceCount = 0;
					long destCount = 0;

					MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(sort).iterator();
					MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(sort).iterator();

					compareCursors(collectionName, sourceCursor, destCursor, ds, true);
					
					logger.debug(String.format("%s - complete. sourceCount: %s, destCount: %s", collectionName,
							sourceCount, destCount));
				}

			} else {
				logger.error(String.format("Destination db not found, name: %s", dbName));
				ds.missingDbs++;
			}
		}
		
		logger.debug(
				String.format("%s dbs compared, %s collections compared, missingDbs %s, idMatches: %s, missingDocs: %s",
						ds.totalDbs, ds.totalCollections, ds.missingDbs, ds.totalMatches, ds.totalMissingDocs));
		return ds;
	}
	
	private void compareCursors(String collectionName, MongoCursor<RawBsonDocument> sourceCursor, 
			MongoCursor<RawBsonDocument> destCursor, DiffSummary ds, boolean compareDocuments) {
		
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
				if (reportMissing) {
					logger.error(String.format("%s - fail: %s missing on source", collectionName, destKey));
				}
				ds.totalMissingDocs++;
				continue;
			} else if (destKey == null) {
				if (reportMissing) {
					logger.error(String.format("%s - fail: %s missing on dest", collectionName, sourceKey));
				}
				ds.totalMissingDocs++;
				continue;
			}

			if (compare < 0) {
				if (reportMissing) {
					logger.error(String.format("%s - fail: %s missing on dest", collectionName, sourceKey));
				}
				ds.totalMissingDocs++;
				destNext = destDoc;
			} else if (compare > 0) {
				if (reportMissing) {
					logger.warn(String.format("%s - fail: %s missing on source", collectionName, destKey));
				}
				ds.totalMissingDocs++;
				sourceNext = sourceDoc;
			} else {
				if (compareDocuments) {
					DiffUtils.compare(collectionName, sourceDoc, sourceKey, destDoc, ds);
				} else {
					if (reportMatches) {
						logger.debug("match on {}, _id: {}", collectionName, sourceKey);
					}
					ds.totalMatches++;
				}
			}
		}
	}
	
	public void compareIdsMapped() {
		DiffSummary ds = new DiffSummary();
		logger.debug("Starting compareIds mode");
		Document sort = new Document("_id", 1);
		
		for (Map.Entry<Namespace, Namespace> entry : mappedNamespaces.entrySet()) {
			Namespace srcNs = entry.getKey();
			Namespace destNs = entry.getValue();
			
			MongoDatabase sourceDb = sourceClient.getDatabase(srcNs.getDatabaseName());
			MongoDatabase destDb = destClient.getDatabase(destNs.getDatabaseName());
			
			MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(srcNs.getCollectionName(),
					RawBsonDocument.class);
			MongoCollection<RawBsonDocument> destColl = destDb.getCollection(destNs.getCollectionName(),
					RawBsonDocument.class);
			long sourceCount = 0;
			long destCount = 0;

			MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find().sort(sort).projection(sort)
					.iterator();
			MongoCursor<RawBsonDocument> destCursor = destColl.find().sort(sort).projection(sort).iterator();
			
			compareCursors(srcNs.getCollectionName(), sourceCursor, destCursor, ds, false);
			
			logger.debug(String.format("%s - complete. sourceCount: %s, destCount: %s", srcNs.getCollectionName(),
					sourceCount, destCount));
		}
		logger.debug(
				String.format("%s dbs compared, %s collections compared, missingDbs %s, idMatches: %s, missingDocs: %s",
						ds.totalDbs, ds.totalCollections, ds.missingDbs, ds.totalMatches, ds.totalMissingDocs));
		
	}

	@SuppressWarnings("unchecked")
	public void compareIds() {
		
		DiffSummary ds = new DiffSummary();
		logger.debug("Starting compareIds mode");
		Document sort = new Document("_id", 1);
		boolean filtered = !includedCollections.isEmpty();

		for (String dbName : sourceDbInfoMap.keySet()) {
			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {

				if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
					continue;
				}
				ds.totalDbs++;

				MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
				MongoDatabase destDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					
					if (filtered) {
						Set<String> colls = includedCollections.get(dbName);
						if (! colls.contains(collectionName)) {
							continue;
						}
					}
					ds.totalCollections++;
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

					compareCursors(collectionName, sourceCursor, destCursor, ds, false);

					logger.debug(String.format("%s - complete. sourceCount: %s, destCount: %s", collectionName,
							sourceCount, destCount));
				}

			} else {
				logger.error(String.format("Destination db not found, name: %s", dbName));
				ds.missingDbs++;
			}
		}
		logger.debug(
				String.format("%s dbs compared, %s collections compared, missingDbs %s, idMatches: %s, missingDocs: %s",
						ds.totalDbs, ds.totalCollections, ds.missingDbs, ds.totalMatches, ds.totalMissingDocs));
	}

	@SuppressWarnings("unchecked")
	public DiffSummary compareShardCounts() {
		
		DiffSummary ds = new DiffSummary();
		logger.debug("Starting compareShardCounts mode");
		boolean filtered = !includedCollections.isEmpty();

		for (String dbName : sourceDbInfoMap.keySet()) {
			Document destInfo = destDbInfoMap.get(dbName);
			if (destInfo != null) {

				if (dbName.equals("admin") || dbName.equals("local") || dbName.equals("config")) {
					continue;
				}
				ds.totalDbs++;

				MongoDatabase sourceDb = sourceClient.getDatabase(dbName);
				MongoDatabase destDb = destClient.getDatabase(dbName);
				MongoIterable<String> sourceCollectionNames = sourceDb.listCollectionNames();
				for (String collectionName : sourceCollectionNames) {
					if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
						continue;
					}
					if (collectionName.startsWith("system.")) {
						continue;
					}
					
					Namespace ns = new Namespace(dbName, collectionName);
					if (filtered && !includedNamespaces.contains(ns)) {
						//logger.debug("include: " + includeNamespaces);
						continue;
					}
					

					long[] result = doCounts(sourceDb, destDb, collectionName);
					sourceTotal += result[0];
					destTotal += result[1];
				}
				logger.debug("Database {} - source count sourceTotal: {}, dest count sourceTotal {}", dbName, sourceTotal, destTotal);
			} else {
				logger.warn(String.format("Destination db not found, name: %s", dbName));
			}
		}
		return ds;
	}
	
	private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName) {
		return doCounts(sourceDb, destDb, collectionName, null);
	}

	private long[] doCounts(MongoDatabase sourceDb, MongoDatabase destDb, String collectionName, Bson query) {

		long[] result = new long[2];
		Long sourceCount = null;
		Long destCount = null;
		if (query == null) {
			sourceCount = sourceDb.getCollection(collectionName).countDocuments();
			destCount = destDb.getCollection(collectionName).countDocuments();
		} else {
			//db.getCollection(collectionName).countDocuments();
			sourceCount = sourceDb.getCollection(collectionName).countDocuments(query);
			destCount = destDb.getCollection(collectionName).countDocuments(query);
		}
		
		result[0] = sourceCount;
		result[1] = destCount;
		
		if (sourceCount.equals(destCount)) {
			logger.debug(String.format("%s.%s count matches: %s", sourceDb.getName(), collectionName, sourceCount));
			return result;
		} else {
			logger.warn(String.format("%s.%s count MISMATCH - source: %s, dest: %s, query: %s", sourceDb.getName(), collectionName,
					sourceCount, destCount, query));
			return result;
		}
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
	
	public void setMappings(String[] mappings) {
		if (mappings != null) {
			for (String mapping : mappings) {
				String[] sourceTarget = mapping.split("\\|");
				logger.debug(sourceTarget[0] + " ==> " + sourceTarget[1]);
				
				Namespace srcNs = new Namespace(sourceTarget[0]);
				Namespace destNs = new Namespace(sourceTarget[1]);
				
				mappedNamespaces.put(srcNs, destNs);
			}
		}
	}

	public void setReportMissing(boolean reportMissing) {
		this.reportMissing = reportMissing;
	}

	public void setReportMatches(boolean reportMatches) {
		this.reportMatches = reportMatches;
	}

	public void setIncludedNamespaces(Set<Namespace> includes) {
		this.includedNamespaces = includes;
		if (includes != null) {
			for (Namespace ns : includes) {
				String nsStr = ns.getNamespace();
				if (nsStr.contains(".")) {
					
					includedNamespaceStrings.add(nsStr);
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
	
	public void setMappedNamespaces(Map<Namespace, Namespace> mappedNamespaces) {
		this.mappedNamespaces = mappedNamespaces;
	}

	public void setGlobalSampleRate(Double globalSampleRate) {
		this.globalSampleRate = globalSampleRate;
	}

}
