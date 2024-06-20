package com.mongodb.corruptutil;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.in;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.model.Namespace;

public class DupeRechecker {

	private static Logger logger = LoggerFactory.getLogger(DupeRechecker.class);

	private Set<Namespace> includeNamespaces = new HashSet<Namespace>();

	private static Options options;

	private MongoClient sourceClient;
	private MongoClient destClient;

	private MongoDatabase archiveDb;

	private Map<Namespace, List<Namespace>> namespacesToCheck = new HashMap<>();

	private Integer startId;
	
	private List<BsonValue> dupesBatch = new ArrayList<>(200);
	
	Bson sort = eq("_id", 1);
	
	int totalDuplicateIds = 0;
	int totalNotDuplicateIds = 0;
	

	public DupeRechecker(String sourceUriStr, String destUriStr, String archiveDbName, String startIdStr) {
		ConnectionString connectionString = new ConnectionString(sourceUriStr);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(connectionString)
				.build();
		sourceClient = MongoClients.create(mongoClientSettings);

		if (destUriStr != null) {
			ConnectionString cs = new ConnectionString(destUriStr);
			MongoClientSettings mcs = MongoClientSettings.builder().applyConnectionString(cs).build();
			destClient = MongoClients.create(mcs);
		} else {
			destClient = sourceClient;
		}

		if (archiveDbName != null) {
			archiveDb = destClient.getDatabase(archiveDbName);
		}

		if (startIdStr != null) {
			startId = Integer.parseInt(startIdStr);
		}

	}

	public void run() throws InterruptedException {

		MongoIterable<String> collNames = archiveDb.listCollectionNames();
		String nsStr = null;
		Namespace ns = null;

		for (String collName : collNames) {

			if (collName.matches(".*_\\d+$")) {
				nsStr = collName.replaceAll("_\\d+$", "");
				ns = new Namespace(nsStr);
			} else {
				continue;
			}

			if (includeNamespaces.isEmpty()) {

				// logger.debug("No namespace filter, checking all namespaces");

				if (namespacesToCheck.containsKey(ns)) {
					namespacesToCheck.get(ns).add(new Namespace(collName));
				} else {
					List<Namespace> inner = new ArrayList<>();
					inner.add(new Namespace(collName));
					namespacesToCheck.put(ns, inner);
				}

			} else if (includeNamespaces.contains(ns)) {

				if (namespacesToCheck.containsKey(ns)) {
					namespacesToCheck.get(ns).add(new Namespace(collName));
				} else {
					List<Namespace> inner = new ArrayList<>();
					inner.add(new Namespace(collName));
					namespacesToCheck.put(ns, inner);
				}
			}
		}

		for (Namespace n : namespacesToCheck.keySet()) {

			logger.debug("will check {} ==> {}", n, namespacesToCheck.get(n));
			
			List<Namespace> nsList = namespacesToCheck.get(n);
			
			MongoCollection<RawBsonDocument> collection = archiveDb.getCollection(nsList.get(0).getNamespace(), RawBsonDocument.class);
			
			MongoCursor<RawBsonDocument> cursor = collection.find().projection(sort).sort(sort).iterator();
			
			MongoDatabase sourceDb = sourceClient.getDatabase(n.getDatabaseName());
			MongoCollection<RawBsonDocument> sourceCollection = sourceDb.getCollection(n.getCollectionName(), RawBsonDocument.class);
			
			Map<BsonValue, Integer> idCountMap = new HashMap<>();
			
			BsonValue lastId = null;
			
			while (cursor.hasNext()) {
                RawBsonDocument fullDoc = cursor.next();
                BsonValue id = null;
                try {
                    id = fullDoc.get("_id");
                } catch (Exception e) {
                    logger.warn(String.format("%s - Error reading doc id, fullDoc: %s, error: %s",
                            collection.getNamespace(), fullDoc, e));
                    continue;
                }
                
                if (id.equals(lastId)) {
                	logger.warn("{} - unexpected duplicate in the archive db for _id: {}", collection.getNamespace(), id);
                } else {
                	dupesBatch.add(id);
                }
                
                if (dupesBatch.size() >= 200) {
                	processDupesBatch(n, sourceCollection, idCountMap);
                }
                
                lastId = id;
    		}
			
			if (dupesBatch.size() > 0) {
            	processDupesBatch(n, sourceCollection, idCountMap);
            }
			
			logger.debug("{}: totalDuplicateIds: {}, totalNotDuplicateIds: {}", totalDuplicateIds, totalNotDuplicateIds);
			
			totalDuplicateIds = 0;
			totalNotDuplicateIds = 0;
			

		}

		logger.debug("CorruptUtil complete");
	}
	
	private void processDupesBatch(Namespace n, MongoCollection<RawBsonDocument> sourceCollection, Map<BsonValue, Integer> idCountMap) {
		Bson query = in("_id", dupesBatch);
		MongoCursor<RawBsonDocument> c2 = sourceCollection.find(query).sort(sort).iterator();
		
		
		while (c2.hasNext()) {
		    RawBsonDocument fullDoc2 = c2.next();
		    BsonValue id2 = null;
		    try {
		        id2 = fullDoc2.get("_id");
		    } catch (Exception e) {
		        logger.warn(String.format("%s - Error reading doc id, fullDoc: %s, error: %s",
		                sourceCollection.getNamespace(), fullDoc2, e));
		        continue;
		    }

		    idCountMap.put(id2, idCountMap.getOrDefault(id2, 0) + 1);
		}
		
		int dupeIdsCount = 0;
		int nonDupeIdsCount = 0;
		Set<BsonValue> nonDupeIdsSet = new LinkedHashSet<>();
		
		// Print the duplicate and non-duplicate IDs
		for (Map.Entry<BsonValue, Integer> entry : idCountMap.entrySet()) {
		    BsonValue key = entry.getKey();
		    int count = entry.getValue();

		    if (count > 1) {
		    	dupeIdsCount++;
		        //logger.warn("{} - duplicate id found for id: {}", collection.getNamespace(), key);
		    } else {
		    	nonDupeIdsCount++;
		    	nonDupeIdsSet.add(key);
		    }
		}
		
		logger.debug("{} duplicate _ids in batch, {} _ids were not duplicate", dupeIdsCount, nonDupeIdsCount);
		
		Bson deleteQuery = in("_id", nonDupeIdsSet);
		for (Namespace nsDelete : namespacesToCheck.get(n)) {
			
			MongoCollection<RawBsonDocument> coll = archiveDb.getCollection(nsDelete.getNamespace(), RawBsonDocument.class);
			DeleteResult dr = coll.deleteMany(deleteQuery);
			logger.debug("{}: deleted {} non-duplicates from archive", nsDelete, dr.getDeletedCount());
		}
		
		
		totalDuplicateIds += dupeIdsCount;
		totalNotDuplicateIds += nonDupeIdsCount;
		
		dupesBatch.clear();
		idCountMap.clear();
	}

	private void addFilters(String[] filters) {
		if (filters == null) {
			return;
		}
		for (String f : filters) {
			Namespace ns = new Namespace(f);
			includeNamespaces.add(ns);
		}
	}

	private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
		options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(Option.builder("s").desc("Source cluster connection uri").hasArgs().longOpt("source")
				.required(true).build());
		options.addOption(Option.builder("d").desc("Destination (archive) cluster connection uri").hasArgs()
				.longOpt("dest").build());
		options.addOption(Option.builder("f").desc("namespace filter").hasArgs().longOpt("filter").build());
		options.addOption(Option.builder("a").desc("archive database name").hasArgs().longOpt("archive").build());

		CommandLineParser parser = new DefaultParser();
		CommandLine line = null;
		try {
			line = parser.parse(options, args);
			if (line.hasOption("help")) {
				printHelpAndExit(options);
			}
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println(e.getMessage());
			printHelpAndExit(options);
		} catch (Exception e) {
			e.printStackTrace();
			printHelpAndExit(options);
		}

		return line;
	}

	private static void printHelpAndExit(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("logParser", options);
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = initializeAndParseCommandLineOptions(args);
		DupeRechecker util = new DupeRechecker(line.getOptionValue("s"), line.getOptionValue("d"),
				line.getOptionValue("a"), line.getOptionValue("i"));

		String[] filters = line.getOptionValues("f");
		util.addFilters(filters);

		util.run();

	}

	public void setArchiveDb(MongoDatabase archiveDb) {
		this.archiveDb = archiveDb;
	}

}
