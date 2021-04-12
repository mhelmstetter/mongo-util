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
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class CorruptUtil {
    
    private static Logger logger = LoggerFactory.getLogger(CorruptUtil.class);
    
    private Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    private Set<String> collectionsBlacklist = new HashSet<>(Arrays.asList("system.indexes", "system.profile"));
    
    private static Options options;
    
    private MongoClient sourceClient;
    private int threads = 4;
    private File outDir;
    
    private ExecutorService executor;
    
    public CorruptUtil(String sourceUriStr) {
        MongoClientURI source = new MongoClientURI(sourceUriStr);
        sourceClient = new MongoClient(source);
    }
    
    public void run() throws InterruptedException {
        executor = Executors.newFixedThreadPool(threads);
        MongoIterable<String> dbNames = sourceClient.listDatabaseNames();
        for (String dbName : dbNames) {
            if (! databasesBlacklist.contains(dbName)) {
                MongoDatabase db = sourceClient.getDatabase(dbName);
                logger.debug("db " + dbName);
                MongoIterable<String> collectionNames = db.listCollectionNames();
                for (String collectionName : collectionNames) {
                    if (collectionsBlacklist.contains(collectionName)) {
                        continue;
                    }
                    MongoCollection<RawBsonDocument> coll = db.getCollection(collectionName, RawBsonDocument.class);
                    Runnable worker = new CorruptFinderWorker(sourceClient, coll, outDir);
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
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source")
                .isRequired(true).create("s"));
        options.addOption(OptionBuilder.withArgName("Zip output path").hasArgs().withLongOpt("outDir")
                .isRequired(true).create("o"));
        options.addOption(OptionBuilder.withArgName("# threads").hasArgs().withLongOpt("threads").create("t"));
        

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
        CorruptUtil util = new CorruptUtil(line.getOptionValue("s"));
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            util.setThreads(threads);
        }
        String outDirStr = line.getOptionValue("o");
        File outDir = new File(outDirStr);
        if (! outDir.canWrite()) {
            throw new IOException("Can't write to outDir " + outDirStr);
        }
        util.setOutDir(outDir);
        util.run();

    }

    private void setOutDir(File outDir) {
        this.outDir = outDir;
    }

}
