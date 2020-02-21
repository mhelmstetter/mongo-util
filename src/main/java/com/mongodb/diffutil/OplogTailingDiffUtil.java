package com.mongodb.diffutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.ShardTimestamp;
import com.mongodb.mongosync.GetLatestOplogTimestampTask;
import com.mongodb.shardsync.ShardClient;


public class OplogTailingDiffUtil {
    
    protected static final Logger logger = LoggerFactory.getLogger(OplogTailingDiffUtil.class);
    
    private static CommandLine line;
    private static Options options;
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    
    private DiffOptions diffOptions;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private Set<String> databasesBlacklist = new HashSet<>(Arrays.asList("system", "local", "config", "admin"));
    
    private ExecutorService executor;
    
    private void initialize(DiffOptions options) {
    	this.diffOptions = options;
        sourceShardClient = new ShardClient("source", diffOptions.getSourceMongoUri());
        sourceShardClient.populateShardMongoClients();
        
        destShardClient = new ShardClient("dest", diffOptions.getDestMongoUri());
        destShardClient.populateShardMongoClients();
       
    }
   
    private void diff() throws InterruptedException, ExecutionException {
    	Collection<Callable<ShardTimestamp>> tasks = new ArrayList<>();
        for (String shardId : sourceShardClient.getShardsMap().keySet()) {
            tasks.add(new OplogTailingDiffTask(shardId, sourceShardClient));
        }
        
        int numThreads = tasks.size();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<ShardTimestamp>> results;
        try {
            results = executor.invokeAll(tasks);
            for(Future<ShardTimestamp> result : results){
                ShardTimestamp shardTimestamp = result.get();
                
                logger.debug("*** " + shardTimestamp.toString());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw(e);
        } finally {
            executor.shutdown();
        }
        logger.debug("Diff complete");
    }
    
    private void execute() {
        
        try {
            diff();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error collecting latest oplog timestamps", e);
            return;
            // TODO exit?
        }
        
        
    }
    
    
    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("OplogTailingDiffUtil", options);
        System.exit(-1);
    }
    
    private static Properties readProperties() {
        Properties prop = new Properties();
        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else  {
            propsFile = new File("diff-util.properties");
            if (! propsFile.exists()) {
                logger.warn("Default config file diff-util.properties not found, using command line options only");
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
    
    @SuppressWarnings("static-access")
    protected static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
       
        options.addOption(OptionBuilder.withArgName("source cluster mongo uri").hasArg().withLongOpt("source").create("s"));
        options.addOption(OptionBuilder.withArgName("destination cluster mongo uri").hasArg().withLongOpt("destination").create("d"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArgs().withLongOpt("config")
                .isRequired(false).create("c"));
        
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
    
    
    public static void main(String args[]) throws Exception {

    	CommandLine line = initializeAndParseCommandLineOptions(args);
    	Properties configFileProps = readProperties();
    	String sourceUri = line.getOptionValue("s", configFileProps.getProperty(SOURCE_URI));
    	String destUri = line.getOptionValue("d", configFileProps.getProperty(DEST_URI));
    	
    	if (sourceUri == null || destUri == null) {
            System.out.println("source and dest options required");
            printHelpAndExit();
        }
        
        OplogTailingDiffUtil sync = new OplogTailingDiffUtil();
        DiffOptions options = new DiffOptions();
        options.setSourceMongoUri(sourceUri);
        options.setDestMongoUri(destUri);
        
        
        
        sync.initialize(options);
        sync.execute();
    }


}
