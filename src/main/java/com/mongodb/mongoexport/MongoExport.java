package com.mongodb.mongoexport;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintStream;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.UuidRepresentation;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

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

@Command(name = "mongoexport", mixinStandardHelpOptions = true, version = "mongoexport 0.1",
description = "Java version of the MongoDB mongoexport command")
public class MongoExport implements Callable<Integer> {
	
	@Option(names = {"--uri"}, description = "mongodb uri connection string", required = true)
    private String uri;
	
	@Option(names = {"-c", "--collection"}, description = "collection to use", required = true)
    private String collectionName;
	
	@Option(names = {"-d", "--db"}, description = "database to use", required = true)
    private String dbName;
	
	@Option(names = {"-o", "--out"}, description = "output file; if not specified, stdout is used", required = false)
    private String outFile;
	
	@Override
    public Integer call() throws Exception { 
        
		ConnectionString connectionString = new ConnectionString(uri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
		MongoClient mongoClient = MongoClients.create(mongoClientSettings);
		
		JsonWriterSettings settings = getJsonWriterSettings();
		
		MongoDatabase db = mongoClient.getDatabase(dbName);
		//mongoClient.getDatabase("admin").runCommand(new Document("ping", 1));
		MongoCursor<Document> cursor = null;
		
		BufferedWriter bw = null;
		if (outFile != null) {
			File file = new File(outFile);
			FileWriter fw = new FileWriter(file);
			  bw = new BufferedWriter(fw);
		}
		
		PrintStream os = null;
		
        long start = System.currentTimeMillis();
        long count = 0;
        try {
        	
        	if (outFile != null) {
        		os = new PrintStream(new BufferedOutputStream(new FileOutputStream(outFile)) );
        	} else {
        		os = new PrintStream(new BufferedOutputStream(System.out) );
        	}
        	
            
            MongoCollection<Document> mongoCollection = db.getCollection(collectionName);
            long totalDocs = mongoCollection.estimatedDocumentCount();
            cursor = mongoCollection.find().iterator();
            
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                os.println(doc.toJson(settings));
                count++;
                if (count % 10000 == 0) {
                    double complete = ((double)count / (double)totalDocs) *100.0;
                    System.out.println(String.format("%s / %s  (%5.1f %%)", count, totalDocs, complete));
                }
            }
        } finally {
            cursor.close();
            os.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        System.out.println(String.format("\nDone dumping %s.%s, %s documents in %f seconds", dbName, collectionName, count, dur));
		
        return 0;
    }
	
	private JsonWriterSettings getJsonWriterSettings() {
		return JsonWriterSettings.builder()
		        .outputMode(JsonMode.RELAXED)
		        .binaryConverter(
		            (value, writer) -> {
		            	if (value.getType() == 4) {
		            		writer.writeString(value.asUuid().toString());
		            	} else if (value.getType() == 3) {
		            		writer.writeString(value.asUuid(UuidRepresentation.JAVA_LEGACY).toString());
		            	} else {
		            		writer.writeString(Base64.getEncoder().encodeToString(value.getData()));
		            	}
		            	
		            })
		        .dateTimeConverter(
		            (value, writer) -> {
		              ZonedDateTime zonedDateTime = Instant.ofEpochMilli(value).atZone(ZoneOffset.UTC);
		              writer.writeString(DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
		            })
		        .decimal128Converter((value, writer) -> writer.writeString(value.toString()))
		        .objectIdConverter((value, writer) -> writer.writeString(value.toHexString()))
		        .symbolConverter((value, writer) -> writer.writeString(value))
		        .timestampConverter((value, writer) -> {
		        	writer.writeStartObject();
		        	writer.writeNumber("t", String.valueOf(value.getTime()));
		        	writer.writeNumber("t", String.valueOf(value.getInc()));
		        	writer.writeEndObject();
		        })
		        .build();
	}

    public static void main(String... args) {
        int exitCode = new CommandLine(new MongoExport()).execute(args);
        System.exit(exitCode);
    }

}
