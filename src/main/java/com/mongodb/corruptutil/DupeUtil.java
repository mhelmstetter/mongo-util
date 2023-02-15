package com.mongodb.corruptutil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class DupeUtil {
    
    private static Logger logger = LoggerFactory.getLogger(DupeUtil.class);
    
    private Set<String> databasesExcludeList = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private Set<String> collectionsExcludeList = new HashSet<>(Arrays.asList("system.indexes", "system.profile"));
    
    private static Options options;
    
    private MongoClient sourceClient;
    private int threads = 4;
    
    private ExecutorService executor;
    
    public DupeUtil(String sourceUriStr) {
    	ConnectionString connectionString = new ConnectionString(sourceUriStr);
    	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
    	sourceClient = MongoClients.create(mongoClientSettings);
    }
    
    public void run() throws InterruptedException {
        executor = Executors.newFixedThreadPool(threads);
        MongoIterable<String> dbNames = sourceClient.listDatabaseNames();
        for (String dbName : dbNames) {
            if (! databasesExcludeList.contains(dbName)) {
                MongoDatabase db = sourceClient.getDatabase(dbName);
                logger.debug("db " + dbName);
                MongoIterable<String> collectionNames = db.listCollectionNames();
                for (String collectionName : collectionNames) {
                    if (collectionsExcludeList.contains(collectionName)) {
                        continue;
                    }
                    MongoCollection<RawBsonDocument> coll = db.getCollection(collectionName, RawBsonDocument.class);
                    Runnable worker = new DupeIdFinderWorker(sourceClient, coll);
                    executor.execute(worker);
                }
            }
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            logger.debug("Waiting for executor to terminate");
            Thread.sleep(1000);
        }
        logger.debug("CorruptUtil complete");
    }
    
    private void setThreads(int threads) {
        this.threads = threads;
    }

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(Option.builder("s").desc("Source cluster connection uri").hasArgs().longOpt("source")
                .required(true).build());
        options.addOption(Option.builder("t").desc("# threads").hasArgs().longOpt("threads").build());
        

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
        DupeUtil util = new DupeUtil(line.getOptionValue("s"));
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            util.setThreads(threads);
        }
        
        util.run();

    }

}
