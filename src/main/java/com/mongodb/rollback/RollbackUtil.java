package com.mongodb.rollback;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bson.Document;
import org.bson.UuidRepresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.opencsv.CSVWriter;

public class RollbackUtil {

	private static final Logger logger = LoggerFactory.getLogger(RollbackUtil.class);

	private Set<String> dbIgnoreList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));

	private static CommandLine line;

	private final static String SOURCE_URI = "source";
	private final static String EXPORT_UUIDS = "exportUuids";

	private String sourceUri;
	private MongoClient mongoClient;

	public void exportUuids(String csvFileName) throws IOException {
		initMongoClient();
		CSVWriter writer = new CSVWriter(new FileWriter(csvFileName));
		String[] header = {"ns", "uuid"};
		writer.writeNext(header);
		
		MongoIterable<String> dbIt = mongoClient.listDatabaseNames();
		for (String dbName : dbIt) {
			if (dbIgnoreList.contains(dbName)) {
				continue;
			}

			MongoDatabase db = mongoClient.getDatabase(dbName);

			for (Document collectionInfo : db.listCollections()) {
				Document info = (Document)collectionInfo.get("info");
				
				UUID uuid = (UUID) info.get("uuid");
				String collectionName = collectionInfo.getString("name");
				
				writer.writeNext(new String[] {dbName + "." + collectionName, uuid.toString()});
			}
		}
		writer.flush();
	}

	private void initMongoClient() {
		ConnectionString connectionString = new ConnectionString(sourceUri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
				.uuidRepresentation(UuidRepresentation.STANDARD)
				.applyConnectionString(connectionString)
				.build();
		mongoClient = MongoClients.create(mongoClientSettings);
	}

	private static void printHelpAndExit(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("rollbackUtil", options);
		System.exit(-1);
	}

	private static void initializeAndParseCommandLineOptions(String[] args) {
		Options options = new Options();
		options.addOption(Option.builder("help").desc("print this message").build());

		options.addOption(
				Option.builder("s").longOpt(SOURCE_URI).desc("Source cluster connection uri").hasArg().build());

		options.addOption(Option.builder("e").longOpt(EXPORT_UUIDS).desc("Export uuids to csv file").hasArg().build());

		CommandLineParser parser = new DefaultParser();

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

		// return line;
	}

	public static void main(String[] args) throws Exception {
		initializeAndParseCommandLineOptions(args);

		RollbackUtil util = new RollbackUtil();
		util.sourceUri = line.getOptionValue(SOURCE_URI);

		if (line.hasOption(EXPORT_UUIDS)) {
			String csvFileName = line.getOptionValue(EXPORT_UUIDS);
			util.exportUuids(csvFileName);
		}

	}

}
