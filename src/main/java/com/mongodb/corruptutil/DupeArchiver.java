package com.mongodb.corruptutil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Namespace;
import com.mongodb.util.BsonUtils;
import com.mongodb.util.CallerBlocksPolicy;

public class DupeArchiver {
	
	private static Logger logger = LoggerFactory.getLogger(DupeArchiver.class);
	
	private final static int ONE_MINUTE = 60 * 1000;
	
	private static Options options;
	
	private final int queueSize = 1000000;
	private final int threads = 8;
	
	BlockingQueue<Runnable> workQueue;
	protected ThreadPoolExecutor executor = null;
	
	private MongoClient destClient;
	private MongoClient sourceClient;
	
	private MongoDatabase archiveDb;
	
	private File sourceFile;
	
	private final static int BATCH_SIZE = 10000;
	List<BsonValue> dupesBatch = new ArrayList<>(BATCH_SIZE);
	
	private final static Pattern valuePattern = Pattern.compile("^(.*?),(.*?)(\\{.*\\})$");
	
    public DupeArchiver(String sourceFileStr, String sourceUriStr, String destUriStr, String archiveDbName) throws IOException {
    	
    	ConnectionString connectionString = new ConnectionString(sourceUriStr);
    	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
    	sourceClient = MongoClients.create(mongoClientSettings);
    	
    	if (destUriStr != null) {
    		ConnectionString cs = new ConnectionString(destUriStr);
        	MongoClientSettings mcs = MongoClientSettings.builder()
                    .applyConnectionString(cs)
                    .build();
        	destClient = MongoClients.create(mcs);
    	} else {
    		destClient = sourceClient;
    	}
    	
    	if (archiveDbName != null) {
        	archiveDb = destClient.getDatabase(archiveDbName);
        }
    	
    	workQueue = new ArrayBlockingQueue<>(queueSize);
		executor = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue, new CallerBlocksPolicy(ONE_MINUTE*5));
		
		sourceFile = new File(sourceFileStr);
		readFile();
		shutdown();
    }
    
    
    private void readFile() throws IOException {
    	 
		BufferedReader reader = null;
		
		
		try {
			reader = new BufferedReader(new FileReader(sourceFile));
	    	
	    	String line = reader.readLine();
	    	Namespace ns = null;
	    	Namespace lastNs = null;
	    	
			while (line != null) {
		    	
		    	Matcher m = valuePattern.matcher(line);
		        if (m.find()) {
		        	
		        	ns = new Namespace(m.group(1));
		        	
			    	if (lastNs != null && !lastNs.equals(ns)) {
			    		submitBatch(lastNs);
			    	}
		        	
		            String bsonType = m.group(2);
		            String valStr = m.group(3);
		            BsonValue idVal = BsonUtils.getValueFromString(bsonType, valStr);
		            dupesBatch.add(idVal);
		            
		        } else {
		        	logger.warn("Invalid line, did not find bson type + value: {}", line);
		        }
		    	
		    	if (dupesBatch.size() >= BATCH_SIZE) {
		    		submitBatch(ns);
		    	}
		    	
				line = reader.readLine();
				lastNs = ns;
			}
			submitBatch(ns);
	    	
		} finally {
			reader.close();
		}
    }
    
    private void submitBatch(Namespace ns) {
    	MongoDatabase db = sourceClient.getDatabase(ns.getDatabaseName());
		MongoCollection<RawBsonDocument> coll = db.getCollection(ns.getCollectionName(), RawBsonDocument.class);
		DupeArchiverTask task = new DupeArchiverTask(coll, archiveDb, Collections.unmodifiableList(dupesBatch));
		logger.debug("submitting batch for ns: {}, size: {}", ns, dupesBatch.size());
		executor.submit(task);
		dupesBatch = new ArrayList<>(BATCH_SIZE);
    }
	
	public void shutdown() {
		logger.debug("DupeArchiver starting shutdown");
		executor.shutdown();
		try {
			Thread.sleep(10000);
			executor.awaitTermination(999, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			logger.warn("DupeArchiver interrupted");
			Thread.currentThread().interrupt();
		}
		logger.debug("DupeArchiver shutdown complete");
	}
	
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(Option.builder("f").desc("Source file").hasArgs().longOpt("file")
                .required(true).build());
        options.addOption(Option.builder("s").desc("Source cluster connection uri").hasArgs().longOpt("source")
                .required(true).build());
        options.addOption(Option.builder("d").desc("Destination (archive) cluster connection uri").hasArgs().longOpt("dest").build());
        //options.addOption(Option.builder("t").desc("# threads").hasArgs().longOpt("threads").build());
        //options.addOption(Option.builder("f").desc("namespace filter").hasArgs().longOpt("filter").build());
        options.addOption(Option.builder("a").desc("archive database name").hasArgs().longOpt("archive").build());
        //options.addOption(Option.builder("i").desc("starting id ($gte)").hasArgs().longOpt("startId").build());
        

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
        DupeArchiver util = new DupeArchiver(line.getOptionValue("f"), line.getOptionValue("s"), 
        		line.getOptionValue("d"), line.getOptionValue("a"));
        
//        String threadsStr = line.getOptionValue("t");
//        if (threadsStr != null) {
//            int threads = Integer.parseInt(threadsStr);
//            util.setThreads(threads);
//        }
        
//        String[] filters = line.getOptionValues("f");
//        util.addFilters(filters);
        

    }

}
