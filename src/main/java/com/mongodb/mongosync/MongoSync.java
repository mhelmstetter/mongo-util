package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "mongosync", mixinStandardHelpOptions = true, version = "mongosync 0.1", description = "mongosync runner", defaultValueProvider = PropertiesDefaultProvider.class)
public class MongoSync implements Callable<Integer> {

	protected static final Logger logger = LoggerFactory.getLogger(MongoSync.class);

	@Option(names = { "--config",
			"-c" }, description = "config file", required = false, defaultValue = "mongo-sync.properties")
	private String configFileStr;

	@Option(names = { "--source" }, description = "source mongodb uri connection string", required = false)
	private String sourceUri;

	@Option(names = { "--dest" }, description = "destination mongodb uri connection string", required = false)
	private String destUri;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;

	private void initialize() throws IOException {

		sourceShardClient = new ShardClient("source", sourceUri);

		sourceShardClient.init();
		sourceShardClient.populateShardMongoClients();
		sourceShardClient.populateCollectionsMap();

		destShardClient = new ShardClient("dest", destUri);
		destShardClient.init();
		destShardClient.populateShardMongoClients();
	}

	@Override
	public Integer call() throws Exception {
		initialize();

		return 0;
	}

	private void shutdown() {

	}

	private static void addShutdownHook(MongoSync sync) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.debug("**** SHUTDOWN *****");
				sync.shutdown();
			}
		}));
	}

	public static void main(String[] args) {
		MongoSync mongoSync = new MongoSync();
		addShutdownHook(mongoSync);

		CommandLine cmd = new CommandLine(mongoSync);
		
		int exitCode = 0;
		try {
			File defaultsFile = new File("mongosync.properties");
            if (defaultsFile.exists()) {
           	 cmd.setDefaultValueProvider(new PropertiesDefaultProvider(defaultsFile));
            }
            
	         ParseResult parseResult = cmd.parseArgs(args);
	         if (!CommandLine.printHelpIfRequested(parseResult)) {
	             
	           
	             
	     		 
	     		
	     		exitCode = mongoSync.call();
	         }
	     } catch (ParameterException ex) {
	         System.err.println(ex.getMessage());
	         ex.getCommandLine().usage(System.err);
	     } catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.exit(exitCode);
	}

}