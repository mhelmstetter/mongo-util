package com.mongodb.catalog;

import java.io.File;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.diff3.DiffUtilApp;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.shardsync.ShardClient;

public class SchemaChangeWatcher extends TimerTask {
	
	
private static Logger logger = LoggerFactory.getLogger(DiffUtilApp.class);
    
    private static Options options;
    private static CommandLine line;
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    
    private String sourceClusterUri;
    private String destClusterUri;
    
    private ShardClient sourceShardClient;
    private ShardClient destShardClient;
    
    private DatabaseCatalog sourceCatalog;
    private DatabaseCatalog destCatalog;
    
    
    public void init() {
    	
    	sourceShardClient = new ShardClient("source", sourceClusterUri);
        destShardClient = new ShardClient("dest", destClusterUri);

        sourceShardClient.init();
        destShardClient.init();
        
        sourceShardClient.populateDatabaseCatalog();
        sourceCatalog = sourceShardClient.getDatabaseCatalog();
        
        destShardClient.populateDatabaseCatalog();
        destCatalog = destShardClient.getDatabaseCatalog();
        
        Timer timer = new Timer("SchemaChangeWatcher timer");
        
        timer.scheduleAtFixedRate(this, 0, 10000);
        
    	
    }
    
    @Override
    public void run()  {
    	
    	logger.debug("run started");
    	
    	sourceShardClient.populateDatabaseCatalog();
    	DatabaseCatalog newSourceCatalog = sourceShardClient.getDatabaseCatalog();
        
        destShardClient.populateDatabaseCatalog();
        DatabaseCatalog newDestCatalog = destShardClient.getDatabaseCatalog();
        
        Set<Collection> newSourceShardedCollections = Sets.difference(newSourceCatalog.getShardedCollections(), sourceCatalog.getShardedCollections());
        logger.debug("new sharded count: {}, old: {}, diff size: {}", newSourceCatalog.getShardedCollections().size(), 
        		sourceCatalog.getShardedCollections().size(), 
        		newSourceShardedCollections.size());
        
        Set<Collection> newSourceUnshardedCollections = Sets.difference(newSourceCatalog.getUnshardedCollections(), sourceCatalog.getUnshardedCollections());
        logger.debug("new unsharded count: {}, old: {}, diff size: {}", newSourceCatalog.getUnshardedCollections().size(), 
        		sourceCatalog.getUnshardedCollections().size(), 
        		newSourceUnshardedCollections.size());
        
        sourceCatalog = newSourceCatalog;
        destCatalog = newDestCatalog;
    	
        logger.debug("run complete");
		
	}
    
    
    private static Configuration readProperties() {
        Configurations configs = new Configurations();
        Configuration defaultConfig = new PropertiesConfiguration();

        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else {
        	propsFile = new File("schema-change-watcher.properties");
            if (! propsFile.exists()) {
            	propsFile = new File("shard-sync.properties");
            }
            if (!propsFile.exists()) {
                logger.warn("Default config files diff-util.properties or shard-sync.properties, not found, using command line options only");
                return defaultConfig;
            }
        }

        try {
            Configuration config = configs.properties(propsFile);
            return config;
        } catch (ConfigurationException e) {
            logger.error("Error loading properties file: " + propsFile, e);
        }
        return defaultConfig;
    }
	
	
	@SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArg().withLongOpt(SOURCE_URI).create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArg().withLongOpt(DEST_URI).create("d"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArg().withLongOpt("config")
                .isRequired(false).create("c"));
        
        CommandLineParser parser = new GnuParser();
        
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
	
	private static String getConfigValue(CommandLine line, Configuration props, String key, String defaultValue) {
        return defaultValue != null && defaultValue.length() > 0 ?
                line.getOptionValue(key, props.getString(key, defaultValue)) :
                line.getOptionValue(key, props.getString(key));
    }
    
    private static String getConfigValue(CommandLine line, Configuration props, String key) {
        return getConfigValue(line, props, key, null);
    }

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }
    
	public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        
        Configuration properties = readProperties();
        
        SchemaChangeWatcher watcher = new SchemaChangeWatcher();
        
        
        watcher.setSourceClusterUri(getConfigValue(line, properties, SOURCE_URI));
        watcher.setDestClusterUri(getConfigValue(line, properties, DEST_URI));
        watcher.init();
    }

	public void setSourceClusterUri(String sourceClusterUri) {
		this.sourceClusterUri = sourceClusterUri;
	}


	public void setDestClusterUri(String destClusterUri) {
		this.destClusterUri = destClusterUri;
	}

}
