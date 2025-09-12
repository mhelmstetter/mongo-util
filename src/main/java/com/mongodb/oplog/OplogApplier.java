package com.mongodb.oplog;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.bson.BSONDecoder;
import org.bson.BSONObject;
import org.bson.BasicBSONDecoder;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.shardsync.ShardConfigSyncApp;

/**
 * 
 * Experimental tool for applying ops Currently intended for troubleshooting
 * ShardSyncUtil / mongomirror oplog application.
 *
 */
public class OplogApplier {

	private static Logger logger = LoggerFactory.getLogger(OplogApplier.class);

	private static Options options;
	private static CommandLine line;

	private final static String SOURCE_URI = "source";
	private final static String DEST_URI = "dest";
	private final static String SOURCE_SHARD = "sourceShard";
	private final static String DEST_SHARD = "destShard";
	private final static String TIMESTAMP = "ts";

	private String sourceClusterUri;
	private String destClusterUri;
	private String sourceShardId;
	private String destShardId;
	private File sourceFile;
	
	private BsonTimestamp timestamp;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;

	private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();

	public void run() {
		logger.debug("OplogApplier starting");

		sourceToDestShardMap.put(sourceShardId, destShardId);

		sourceShardClient = new ShardClient("source", sourceClusterUri, sourceToDestShardMap.keySet());
		destShardClient = new ShardClient("dest", destClusterUri, sourceToDestShardMap.values());

		sourceShardClient.init();
		destShardClient.init();
		sourceShardClient.populateShardMongoClients();
		destShardClient.populateShardMongoClients();

		MongoClient sourceClient = sourceShardClient.getShardMongoClient(sourceShardId);
		MongoClient destClient = destShardClient.getShardMongoClient(destShardId);
		
		MongoDatabase local = sourceClient.getDatabase("local");
        MongoCollection<Document> oplog = local.getCollection("oplog.rs", Document.class);
        
        List<Document> opsList = new ArrayList<>(1);
        
        MongoCursor<Document> cursor = null;
        Bson query = and(gte("ts", timestamp), ne("op", "n"));
        long start = System.currentTimeMillis();
        long count = 0;
        long errorCount = 0;
        try {
            //cursor = oplog.find(query).noCursorTimeout(true).cursorType(CursorType.TailableAwait).iterator();
        	cursor = oplog.find(query).sort(new Document("$natural", 1)).noCursorTimeout(true).iterator();
            while (cursor.hasNext()) {
                Document doc = cursor.next();
                String ns = doc.getString("ns");
                if (ns.startsWith("config.")) {
                	continue;
                }
                String dbName = StringUtils.substringBefore(ns, ".");
                doc.remove("ui");
                //logger.debug("applying: " + doc);
                
                opsList.clear();
                opsList.add(doc);
                Document applyOps = new Document("applyOps", opsList);
                try {
                	Document result = destClient.getDatabase(dbName).runCommand(applyOps);
                	//logger.debug("result: " + result);
                } catch (MongoCommandException mce) {
                	errorCount++;
                	logger.error("error applying: " + mce.getMessage());
                	logger.debug("op: " + doc);
                }
                
                count++;
                if (count % 100 == 0) {
                	logger.debug(String.format("Executed %s applyOps, errorCount: %s", count, errorCount));
                }
            }
            
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        logger.debug(String.format("Executed %s applyOps in %f seconds, errorCount: %s", count, dur, errorCount));
    }
	
	public void runFile() throws FileNotFoundException {
		logger.debug("OplogApplier starting");

		//sourceToDestShardMap.put(sourceShardId, destShardId);

		//sourceShardClient = new ShardClient("source", sourceClusterUri, sourceToDestShardMap.keySet());
		destShardClient = new ShardClient("dest", destClusterUri, sourceToDestShardMap.values());

		//sourceShardClient.init();
		destShardClient.init();
		//sourceShardClient.populateShardMongoClients();
		destShardClient.populateShardMongoClients();

		//MongoClient sourceClient = sourceShardClient.getShardMongoClient(sourceShardId);
		MongoClient destClient = destShardClient.getShardMongoClient(destShardId);
		
		
        long start = System.currentTimeMillis();
        long count = 0;
        long errorCount = 0;
        
        InputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFile));
        
        List<BSONObject> opsList = new ArrayList<>(1);
        BSONDecoder decoder = new BasicBSONDecoder();
        try {
            while (inputStream.available() > 0) {
                
            	
                BSONObject obj = decoder.readObject(inputStream);
                if(obj == null){
                    break;
                }
                
                String ns = (String)obj.get("ns");
                if (ns.startsWith("config.") || ns.contains(".tmp.")) {
                	continue;
                }
                String dbName = StringUtils.substringBefore(ns, ".");
                obj.removeField("ui");
                
                opsList.clear();
                opsList.add(obj);
                Document applyOps = new Document("applyOps", opsList);
                try {
                	Document result = destClient.getDatabase(dbName).runCommand(applyOps);
                	logger.debug("result: " + result);
                } catch (MongoCommandException mce) {
                	errorCount++;
                	logger.error("error applying: " + mce.getMessage());
                	logger.debug("op: " + obj);
                }
                
                count++;
                if (count % 100 == 0) {
                	logger.debug(String.format("Executed %s applyOps, errorCount: %s", count, errorCount));
                }
                
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (IOException e) {
            }
        }
        System.err.println(String.format("%s objects read", count));
        
        
        
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        logger.debug(String.format("Executed %s applyOps in %f seconds, errorCount: %s", count, dur, errorCount));
    }
        

	@SuppressWarnings("static-access")
	private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
		options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArgs().withLongOpt("config")
				.isRequired(false).create("c"));
		options.addOption(
				OptionBuilder.withArgName("source shard id").hasArg().withLongOpt(SOURCE_SHARD).create("s"));
		options.addOption(
				OptionBuilder.withArgName("dest shard id").hasArg().withLongOpt(DEST_SHARD).isRequired().create("d"));
		options.addOption(
				OptionBuilder.withArgName("oplog timestamp <time>,<increment>").hasArg().withLongOpt(TIMESTAMP).create());
		
		options.addOption(
				OptionBuilder.withArgName("source file").hasArg().withLongOpt("sourceFile").create("f"));

		CommandLineParser parser = new GnuParser();
		try {
			line = parser.parse(options, args);
			if (line.hasOption("help")) {
				printHelpAndExit();
			}
		} catch (org.apache.commons.cli.ParseException e) {
			System.out.println(e.getMessage());
			printHelpAndExit();
		} catch (Exception e) {
			e.printStackTrace();
			printHelpAndExit();
		}

		return line;
	}

	private static Properties readProperties() {
		Properties prop = new Properties();
		File propsFile = null;
		if (line.hasOption("c")) {
			propsFile = new File(line.getOptionValue("c"));
		} else {
			propsFile = new File(ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE);
			if (!propsFile.exists()) {
				logger.warn("Default config file {} not found, using command line options only", ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE);
				return prop;
			}
		}
		
		try (InputStream input = new FileInputStream(propsFile)) {
			prop.load(input);
		} catch (IOException ioe) {
			logger.error("Error loading properties file: " + propsFile, ioe);
		}
		return prop;
	}

	private static void printHelpAndExit() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("logParser", options);
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = initializeAndParseCommandLineOptions(args);

		Properties configFileProps = readProperties();

		OplogApplier oplog = new OplogApplier();
		oplog.setSourceClusterUri(configFileProps.getProperty(SOURCE_URI));
		oplog.setDestClusterUri(configFileProps.getProperty(DEST_URI));
		oplog.setSourceShardId(line.getOptionValue(SOURCE_SHARD));
		oplog.setDestShardId(line.getOptionValue(DEST_SHARD));
		
		String timestamp = line.getOptionValue(TIMESTAMP);
		if (timestamp != null) {
			String[] tsParts = timestamp.split(",");
			int time = Integer.parseInt(tsParts[0]);
			int inc = Integer.parseInt(tsParts[1]);
			oplog.setTimestamp(new BsonTimestamp(time, inc));
		}
		
		
		String sourceFileName = line.getOptionValue("sourceFile");
		if (sourceFileName != null) {
			oplog.setSourceFile(new File(sourceFileName));
			oplog.runFile();
		} else {
			oplog.run();
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

	public void setSourceShardId(String sourceShardId) {
		this.sourceShardId = sourceShardId;
	}

	public void setDestShardId(String destShardId) {
		this.destShardId = destShardId;
	}


	public void setTimestamp(BsonTimestamp timestamp) {
		this.timestamp = timestamp;
	}


	public void setSourceFile(File sourceFile) {
		this.sourceFile = sourceFile;
	}

}
