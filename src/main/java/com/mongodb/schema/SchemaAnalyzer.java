package com.mongodb.schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.StringUtils;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "schemaAnalyzer", mixinStandardHelpOptions = true, version = "schemaAnalyzer 0.1")
public class SchemaAnalyzer implements Callable<Integer> {
	
	private static Logger logger = LoggerFactory.getLogger(SchemaAnalyzer.class);
	
	private Set<String> dbIgnoreList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;
	
	@Option(names = {"--list"}, description = "list dbs/collections and counts", required = false)
    private boolean list;
	
	@Option(names = {"--ns"}, description = "namespace (e.g. <db_name>.<collection_name> )", required = false)
    private String namespace;
	
	@Option(names = {"--limit"}, description = "limit number of documents to analyze (default all documents)", required = false)
    private Integer limit;
	
	LinkedHashMap<String, BsonType> keyTypeMap = null;
	
	MongoClient mongoClient;
	
	private void connect() {
		ConnectionString connectionString = new ConnectionString(uri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
		mongoClient = MongoClients.create(mongoClientSettings);
	}
	
	private void list() {
		Document listDatabases = new Document("listDatabases", 1);
		Document dbListResult = mongoClient.getDatabase("admin").runCommand(listDatabases);
		List<Document> databasesList = dbListResult.getList("databases", Document.class);
		
		System.out.println(String.format("%-40s %-20s", "Namespace", "Count"));
		System.out.println(String.format("%-40s %-20s", "----------------------------------------", "--------------------"));
		
		for (Document dbInfo : databasesList) {
			String dbName = dbInfo.getString("name");
			if (dbIgnoreList.contains(dbName)) {
				continue;
			}
			MongoDatabase db = mongoClient.getDatabase(dbName);
			
			for (Document collectionInfo : db.listCollections()) {
				String collectionName = collectionInfo.getString("name");
				String ns = dbName + "." + collectionName;
				MongoCollection<Document> coll = db.getCollection(collectionName);
				long count =  coll.estimatedDocumentCount();
				System.out.println(String.format("%-40s %,20d", ns, count));
				
			}
			
		}
	}
	
	private void analyze() throws IOException {
		String dbName = StringUtils.substringBefore(namespace, ".");
		String collectionName = StringUtils.substringAfter(namespace, ".");
		
		MongoDatabase db = mongoClient.getDatabase(dbName);
		mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
		MongoCursor<BsonDocument> cursor = null;
        long start = System.currentTimeMillis();
        long count = 0;
        try {
            
            MongoCollection<BsonDocument> mongoCollection = db.getCollection(collectionName, BsonDocument.class);
            long totalDocs = mongoCollection.estimatedDocumentCount();
            logger.debug(String.format("Total documents for %s: %s", namespace, totalDocs));
            
            if (limit != null) {
            	 cursor = mongoCollection.find().limit(limit).iterator();
            } else {
            	cursor = mongoCollection.find().iterator();
            }
           
            		
            keyTypeMap = new LinkedHashMap<>();
            
            Set<String> keys = null;
            Set<String> lastKeys = null;
            BsonDocument doc = null;
            
            while (cursor.hasNext()) {
            	doc = cursor.next();
                
                keys = doc.keySet();
                
                boolean sameKeysAsLast = keys.equals(lastKeys);
                
                if (! sameKeysAsLast) {
                	for (String key :  keys) {
                    	BsonValue val = doc.get(key);
                    	BsonType type = val.getBsonType();
                    	keyTypeMap.put(key, type);
                    }
                }
                
                count++;
                if (count % 100000 == 0) {
                    double complete = count/totalDocs*100.0;
                    logger.debug(String.format("%s / %s  (%f)", count, totalDocs, complete));
                }
            }
            
            //System.out.println(doc.toJson());
            //System.out.println(keyTypeMap);
            
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        logger.debug(String.format("\nDone dumping %s.%s, %s documents in %f seconds", dbName, collectionName, count, dur));
        
        CSharpClassWriter classWriter = new CSharpClassWriter(collectionName);
        classWriter.writeClass(keyTypeMap);
	}
	
	
	@Override
    public Integer call() throws Exception { 
        
		connect();
		
		if (list) {
			list();
		} else {
			analyze();
		}
		
        return 0;
    }

    public static void main(String... args) {
        int exitCode = new CommandLine(new SchemaAnalyzer()).execute(args);
        System.exit(exitCode);
    }

}
