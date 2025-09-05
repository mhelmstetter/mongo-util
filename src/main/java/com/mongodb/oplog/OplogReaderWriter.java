package com.mongodb.oplog;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.ne;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.BsonDocument;
import org.bson.UuidRepresentation;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.util.DatabaseUtil;

import com.mongodb.ConnectionString;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoClientSettings;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;
import com.mongodb.mongosync.BulkWriteOutput;
import com.mongodb.shardsync.ShardClient;

public class OplogReaderWriter {

	private static Logger logger = LoggerFactory.getLogger(OplogReaderWriter.class);

	private final static BulkWriteOptions orderedBulkWriteOptions = new BulkWriteOptions();

	private static Options options;

	private final static int BATCH_SIZE = 1000;

	private final static String SOURCE_URI = "source";
	private final static String DEST_URI = "dest";
	private String sourceUri;
	private String destUri;

	// private ShardClient sourceShardClient;
	// private ShardClient destShardClient;

	private MongoClient sourceClient;
	private MongoClient destClient;

	BulkWriteOutput results = new BulkWriteOutput();

	public OplogReaderWriter(String sourceUri, String destUri) {
		this.sourceUri = sourceUri;
		this.destUri = destUri;
	}

	public void run() {
		// sourceShardClient = new ShardClient("source", sourceUri);
		// destShardClient = new ShardClient("dest", destUri);

		ConnectionString sourceCs = new ConnectionString(sourceUri);
		MongoClientSettings mongoClientSettings = MongoClientSettings.builder().applyConnectionString(sourceCs)
				.uuidRepresentation(UuidRepresentation.STANDARD).build();
		sourceClient = MongoClients.create(mongoClientSettings);

		ConnectionString destCs = new ConnectionString(destUri);
		MongoClientSettings destMongoClientSettings = MongoClientSettings.builder().applyConnectionString(destCs)
				.uuidRepresentation(UuidRepresentation.STANDARD).build();
		destClient = MongoClients.create(destMongoClientSettings);

		MongoDatabase db = sourceClient.getDatabase("urban");
		MongoCollection<BsonDocument> oplog = db.getCollection("oplog", BsonDocument.class);

		MongoCursor<BsonDocument> cursor = null;

		// MongoCursor<Document> cursor = null;
		Bson query = and(ne("op", "n"));

		Map<Namespace, List<WriteModel<BsonDocument>>> writeModelsMap = new HashMap<>();

		try {
			cursor = oplog.find(query).noCursorTimeout(true).iterator();
			while (cursor.hasNext()) {
				BsonDocument doc = cursor.next();
				String op = doc.getString("op").getValue();

				if (op.equals("n") || op.equals("c")) {
					continue;
				}

				String nsString = doc.getString("ns").getValue();
				Namespace ns = new Namespace(nsString);
				if (DatabaseUtil.isSystemDatabase(ns.getDatabaseName()) && !ns.getDatabaseName().equals("local")) {
					continue;
				}

				List<WriteModel<BsonDocument>> writeModels = writeModelsMap.get(ns);
				if (writeModels == null) {
					writeModels = new ArrayList<>(BATCH_SIZE);
					writeModelsMap.put(ns, writeModels);
				}

				WriteModel<BsonDocument> model = ApplyOperationsHelper.getWriteModelForOperation(doc, false);
				if (model != null) {
					writeModels.add(model);

					if (writeModels.size() >= BATCH_SIZE) {
						flush(ns, writeModels);
					}

				} else {
					// if the command is $cmd for create index or create collection, there would not
					// be any write model.
					logger.warn("ignoring oplog entry. could not convert the document to model. Given document is {}",
							doc.toJson());
				}
			}

		} finally {
			cursor.close();
		}

	}

	private void flush(Namespace ns, List<WriteModel<BsonDocument>> writeModels) {

		MongoCollection<BsonDocument> collection = destClient.getDatabase(ns.getDatabaseName())
				.getCollection(ns.getCollectionName(), BsonDocument.class);

		BulkWriteResult bulkWriteResult = null;
		try {
			bulkWriteResult = collection.bulkWrite(writeModels, orderedBulkWriteOptions);
			writeModels.clear();
			results.increment(bulkWriteResult);
			logger.debug(results.toString());

		} catch (MongoBulkWriteException err) {
			List<BulkWriteError> errors = err.getWriteErrors();
			bulkWriteResult = err.getWriteResult();
			if (errors.size() == writeModels.size()) {
				logger.error("{} bulk write errors, all {} operations failed: {}", err);
			} else {
				for (BulkWriteError bwe : errors) {
					int index = bwe.getIndex();
					writeModels.remove(index);
				}
				flush(ns, writeModels);
			}
		} catch (Exception ex) {
			logger.error("{} unknown error: {}", ex.getMessage(), ex);
		}

	}

	@SuppressWarnings("static-access")
	private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
		options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(
				OptionBuilder.withArgName("source cluster mongo uri").hasArg().withLongOpt(SOURCE_URI).isRequired(true).create("s"));
		options.addOption(
				OptionBuilder.withArgName("destination cluster mongo uri").hasArg().withLongOpt(DEST_URI).isRequired(true).create("d"));

		CommandLineParser parser = new GnuParser();
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
		String sourceUri = line.getOptionValue("s");
		String destUri = line.getOptionValue("d");
		OplogReaderWriter op = new OplogReaderWriter(sourceUri, destUri);
		op.run();
	}

}
