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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
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
import com.mongodb.util.CallerBlocksPolicy;

public class DupeArchiver {
	
	private static Logger logger = LoggerFactory.getLogger(DupeArchiver.class);
	
	private final static int ONE_MINUTE = 60 * 1000;
	
	private static Options options;
	
	private final int queueSize = 100000;
	private final int threads = 8;
	
	BlockingQueue<Runnable> workQueue;
	protected ThreadPoolExecutor executor = null;
	
	private MongoClient destClient;
	private MongoClient sourceClient;
	
	private MongoDatabase archiveDb;
	
	private File sourceFile;
	
	private final static int BATCH_SIZE = 1000;
	
    public DupeArchiver(String sourceFileStr, String sourceUriStr, String destUriStr, String archiveDbName) throws IOException {
    	
    	ConnectionString connectionString = new ConnectionString(sourceUriStr);
    	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
    	sourceClient = MongoClients.create(mongoClientSettings);
    	
    	ConnectionString cs = new ConnectionString(destUriStr);
    	MongoClientSettings mcs = MongoClientSettings.builder()
                .applyConnectionString(cs)
                .build();
    	destClient = MongoClients.create(mcs);
    	
    	
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
	    	
	    	List<BsonValue> dupesBatch = new ArrayList<>(BATCH_SIZE);

			while (line != null) {
				String[] splits = line.split(",");
		    	Namespace ns = new Namespace(splits[0]);
		    	String id = splits[1];
		    	BsonValue idVal = new BsonInt64(Long.parseLong(id));
		    	dupesBatch.add(idVal);
		    	
		    	if (dupesBatch.size() >= BATCH_SIZE) {
		    		MongoDatabase db = sourceClient.getDatabase(ns.getDatabaseName());
		    		MongoCollection<RawBsonDocument> coll = db.getCollection(ns.getCollectionName(), RawBsonDocument.class);
		    		DupeArchiverTask task = new DupeArchiverTask(coll, archiveDb, Collections.unmodifiableList(dupesBatch));
		    		executor.submit(task);
		    	}
		    	
				line = reader.readLine();
			}
	    	
		} finally {
			reader.close();
		}
    }
	
	public void shutdown() {
		logger.debug("ShardedCollectionCloneWorker starting shutdown");
		executor.shutdown();
		try {
			executor.awaitTermination(999, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			logger.warn("ShardedCollectionCloneWorker interrupted");
			Thread.currentThread().interrupt();
		}
		logger.debug("ShardedCollectionCloneWorker shutdown complete");
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
