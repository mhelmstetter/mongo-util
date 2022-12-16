package com.mongodb.rollback;

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
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
import com.mongodb.model.Namespace;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.CSVWriter;

public class RollbackUtil {

	private static final Logger logger = LoggerFactory.getLogger(RollbackUtil.class);

	private Set<String> dbIgnoreList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));

	private static CommandLine line;

	private final static String SOURCE_URI = "source";
	private final static String EXPORT_UUIDS = "exportUuids";
	private final static String UUID_FILE = "uuidFile";
	private final static String ROLLBACK_DIR = "rollbackDir";

	private String sourceUri;
	private MongoClient mongoClient;
	private Map<String, Namespace> uuidToNamespaceMap;

	public void exportUuids(String csvFileName) throws IOException {
		initMongoClient();
		CSVWriter writer = new CSVWriter(new FileWriter(csvFileName));
		String[] header = { "ns", "uuid" };
		writer.writeNext(header);

		MongoIterable<String> dbIt = mongoClient.listDatabaseNames();
		for (String dbName : dbIt) {

			MongoDatabase db = mongoClient.getDatabase(dbName);

			for (Document collectionInfo : db.listCollections()) {
				Document info = (Document) collectionInfo.get("info");

				UUID uuid = (UUID) info.get("uuid");
				String collectionName = collectionInfo.getString("name");

				writer.writeNext(new String[] { dbName + "." + collectionName, uuid.toString() });
			}
		}
		writer.flush();
	}

	private void loadUuidMap(Path filePath) throws IOException {
		
		uuidToNamespaceMap = new HashMap<>();
		
		try (Reader reader = Files.newBufferedReader(filePath)) {
			CSVParser parser = new CSVParserBuilder().withSeparator(',').withIgnoreQuotations(true).build();

			CSVReader csvReader = new CSVReaderBuilder(reader).withSkipLines(0).withCSVParser(parser).build();
			
			String[] line;
            while ((line = csvReader.readNext()) != null) {
                String nsStr = line[0];
                String uuid = line[1];
                Namespace ns = new Namespace(nsStr);
                uuidToNamespaceMap.put(uuid, ns);
            }
		}

		

	}

	public void processRollbacks(String rollbackDir, String uuidFile) throws IOException {

		loadUuidMap(Paths.get(uuidFile));
		
		Path rollbackPath = Paths.get(rollbackDir);

		if (!Files.isDirectory(rollbackPath)) {
			throw new IllegalArgumentException("Path must be a directory!");
		}

		List<Path> result;
		try (Stream<Path> walk = Files.walk(rollbackPath)) {
			result = walk.filter(Files::isRegularFile) // is a file
					.filter(p -> p.getFileName().toString().endsWith(".bson")).collect(Collectors.toList());
		}

		for (Path p : result) {
			readBson(p);
		}
	}

	private void readBson(Path p) throws IOException {
		InputStream stream = null;
		try {
			stream = Files.newInputStream(p);

			InputStream inputStream = new BufferedInputStream(stream);
			BSONDecoder decoder = new BasicBSONDecoder();

			String fileName = p.getFileName().toString();
			String uuid = StringUtils.substringBefore(fileName, ".");
			Namespace ns = uuidToNamespaceMap.get(uuid);
			if (ns == null) {
				logger.error("Namespace not found for uuid {}", uuid);
			}
			while (inputStream.available() > 0) {

				BSONObject obj = decoder.readObject(inputStream);
				if (obj == null) {
					break;
				}

				Object id = obj.get("_id");
				System.out.println(id + " " + ns);
			}

		} catch (FileNotFoundException e) {
			logger.error("file not found", e);
		} finally {
			stream.close();
		}

	}

	private void initMongoClient() {
		ConnectionString connectionString = new ConnectionString(sourceUri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
				.uuidRepresentation(UuidRepresentation.STANDARD).applyConnectionString(connectionString).build();
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
		options.addOption(Option.builder("f").longOpt(UUID_FILE).desc("Input uuid csv file").hasArg().build());

		options.addOption(Option.builder("r").longOpt(ROLLBACK_DIR)
				.desc("Rollback directory to search for rollback files").hasArg().build());

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
		} else if (line.hasOption(ROLLBACK_DIR)) {
			String uuidFile = line.getOptionValue(UUID_FILE);
			util.processRollbacks(line.getOptionValue(ROLLBACK_DIR), uuidFile);
		}

	}

}
