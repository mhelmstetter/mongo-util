package com.mongodb.diffutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Shard;
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
    
    private Map<String, String> sourceToDestShardMap = new HashMap<String, String>();
    
    private ExecutorService executor;
    
    private void initialize(DiffOptions options) {
    	this.diffOptions = options;
    	initializeShardMappings();
        sourceShardClient.populateShardMongoClients();
        destShardClient.populateShardMongoClients();
    }
    
	public void initializeShardMappings() {
		logger.debug("Start initializeShardMappings()");
		
		if (diffOptions.getShardMap() != null) {
			// shardMap is for doing an uneven shard mapping, e.g. 10 shards on source
			// down to 5 shards on destination
			logger.debug("Custom shard mapping");
			
			for (String mapping : diffOptions.getShardMap()) {
				String[] mappings = mapping.split("\\|");
				logger.debug(mappings[0] + " ==> " + mappings[1]);
				sourceToDestShardMap.put(mappings[0], mappings[1]);
			}
			
			sourceShardClient = new ShardClient("source", diffOptions.getSourceMongoUri(), sourceToDestShardMap.keySet());
			destShardClient = new ShardClient("dest", diffOptions.getDestMongoUri(), sourceToDestShardMap.values());
			sourceShardClient.init();
			destShardClient.init();
			
		} else {
			logger.debug("Default 1:1 shard mapping");
			
			sourceShardClient = new ShardClient("source", diffOptions.getSourceMongoUri());
			destShardClient = new ShardClient("dest", diffOptions.getDestMongoUri());
			sourceShardClient.init();
			destShardClient.init();
			
			logger.debug("Source shard count: " + sourceShardClient.getShardsMap().size());
			// default, just match up the shards 1:1
			int index = 0;
			List<Shard> destList = new ArrayList<Shard>(destShardClient.getShardsMap().values());
			for (Iterator<Shard> i = sourceShardClient.getShardsMap().values().iterator(); i.hasNext();) {
				Shard sourceShard = i.next();
				Shard destShard = destList.get(index);
				if (destShard != null) {
					logger.debug(sourceShard.getId() + " ==> " + destShard.getId());
					sourceToDestShardMap.put(sourceShard.getId(), destShard.getId());
				}
				index++;
			}
		}
	}
   
    private void diff() throws InterruptedException, ExecutionException {
    	Collection<Callable<OplogTailingDiffTaskResult>> tasks = new ArrayList<>();
    	List<Future<OplogTailingDiffTaskResult>> futures = new ArrayList<>();
    	executor = Executors.newFixedThreadPool(sourceShardClient.getShardsMap().size());
        for (String sourceShardId : sourceShardClient.getShardsMap().keySet()) {
        	String destShardId = sourceToDestShardMap.get(sourceShardId);
        	
        	OplogTailingDiffTask task = new OplogTailingDiffTask(sourceShardId, destShardId, sourceShardClient, destShardClient, diffOptions.getThreads(), diffOptions.getQueueSize());
        	futures.add(executor.submit(task));
            
        	//tasks.add(new OplogTailingDiffTask(sourceShardId, destShardId, sourceShardClient, destShardClient, diffOptions.getThreads(), diffOptions.getQueueSize()));
        }
        
        executor.shutdown();
        
      for(Future<OplogTailingDiffTaskResult> future : futures) {
    	OplogTailingDiffTaskResult result = future.get();
        
        logger.debug("*** " + result.toString());
    }
        
//        int numThreads = tasks.size();
//        ExecutorService oplogTailExecutor = Executors.newFixedThreadPool(numThreads);
//        List<Future<OplogTailingDiffTaskResult>> results;
//        try {
//            results = oplogTailExecutor.invokeAll(tasks);
//            for(Future<OplogTailingDiffTaskResult> future : results){
//            	OplogTailingDiffTaskResult result = future.get();
//                
//                logger.debug("*** " + result.toString());
//            }
//        } catch (InterruptedException | ExecutionException e) {
//            throw(e);
//        } finally {
//            oplogTailExecutor.shutdown();
//        }
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
