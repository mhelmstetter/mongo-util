package com.mongodb.catalog;

import static com.mongodb.shardsync.BaseConfiguration.Constants.DEST_URI;
import static com.mongodb.shardsync.BaseConfiguration.Constants.SOURCE_URI;
import static com.mongodb.util.ConfigUtils.getConfigValue;
import static com.mongodb.util.ConfigUtils.getConfigValues;

import java.io.File;
import java.util.Timer;

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

import com.mongodb.shardsync.ShardConfigSync;
import com.mongodb.shardsync.SyncConfiguration;

import jakarta.mail.internet.AddressException;

public class SchemaChangeWatcher {
	
	
	private static Logger logger = LoggerFactory.getLogger(SchemaChangeWatcher.class);
    
    private static Options options;
    private static CommandLine line;
    
    private final static String CLUSTER_URI = "uri";
    private final static String CONFIG_URI_KEYS = "uriKey";
    private final static String CLUSTER_NAMES = "clusterName";
    private final static String CHECK_INTERVAL_SECONDS = "checkIntervalSeconds";
    private static final String EMAIL_RECIPIENTS = "emailReportRecipients";
    private static final String EMAIL_SMTP_HOST = "emailSmtpHost";
    private static final String EMAIL_SMTP_PORT = "emailSmtpPort";
    private static final String EMAIL_SMTP_TLS = "emailSmtpTls";
    private static final String EMAIL_SMTP_AUTH = "emailSmtpAuth";
    private static final String EMAIL_FROM = "emailFrom";
    private static final String EMAIL_SMTP_PASSWORD = "emailSmtpPassword";
    private static final String DEFAULT_EMAIL_SMTP_HOST = "smtp.gmail.com";
    private static final String DEFAULT_EMAIL_SMTP_PORT = "587";
    private static final String DEFAULT_EMAIL_SMTP_TLS = "true";
    private static final String DEFAULT_EMAIL_SMTP_AUTH = "true";
    
    private static final String WATCH_COLLECTION_UUIDS = "watchCollectionUuids";
	
	private EmailSender emailSender;
    
    private String[] clusterUris;
    private String[] clusterUriKeys;
    private String[] clusterNames;
    
    private long checkIntervalSeconds;
    
    private boolean watchCollectionUuids;
    
    private ShardConfigSync sync;
    
    public SchemaChangeWatcher(Configuration properties) {
    	SyncConfiguration config = new SyncConfiguration();
        config.setSourceClusterUri(line.getOptionValue("s", properties.getString(SOURCE_URI)));
        
        String destUri = line.getOptionValue("d", properties.getString(DEST_URI));
        if (destUri != null) {
        	config.setDestClusterUri(destUri);
        }
        
        
        sync = new ShardConfigSync(config);
        sync.initialize();
    }
    
    public void init() {
    	logger.debug("SchemaChangeWatcher init(), {} clusterUris configured", clusterUris.length);
    	
    	String name;
    	int i = 0;
    	for (String clusterUri : clusterUris) {
    		if (clusterNames.length == clusterUris.length) {
    			name = clusterNames[i++];
    		} else if (clusterUriKeys.length == clusterUris.length) {
    			name = clusterUriKeys[i++];
    		} else {
    			name = clusterUri;
    		}
    		SchemaChangeWatcherTask sourceTask = new SchemaChangeWatcherTask(name, clusterUri, emailSender);
            Timer timer = new Timer("SchemaChangeWatcher timer");
            timer.scheduleAtFixedRate(sourceTask, 0, checkIntervalSeconds*1000L);
            
            if (watchCollectionUuids) {
            	CollectionUuidWatcherTask uuidTask = new CollectionUuidWatcherTask(name, sync, emailSender);
                Timer t2 = new Timer("CollectionUuidWatcher timer");
                t2.scheduleAtFixedRate(uuidTask, 0, checkIntervalSeconds*1000L);
            }
    	}
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
        options.addOption(OptionBuilder.withArgName("Cluster connection uri").hasArgs().withLongOpt(CLUSTER_URI).create("u"));
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
	
	private void initEmailReportConfig(CommandLine line, Configuration props) throws AddressException {
		emailSender = new EmailSender();
        String emailReportRecipientsRaw = getConfigValue(line, props, EMAIL_RECIPIENTS);
        if (emailReportRecipientsRaw == null) {
        	logger.debug("{} not configured, skipping mongomirror email reporting config", EMAIL_RECIPIENTS);
        	return;
        }
        String[] rawSplits = emailReportRecipientsRaw.split(",");
        for (String raw : rawSplits) {
            if (raw.length() > 0) {
            	emailSender.addEmailRecipient(raw.trim());
            } else {
            	throw new AddressException("No email recipients specified");
            }
        }

        emailSender.setSmtpHost(getConfigValue(line, props, EMAIL_SMTP_HOST, DEFAULT_EMAIL_SMTP_HOST));
        emailSender.setSmtpPort(Integer.parseInt(getConfigValue(line, props, EMAIL_SMTP_PORT, DEFAULT_EMAIL_SMTP_PORT)));
        emailSender.setSmtpTls(Boolean.parseBoolean(getConfigValue(line, props, EMAIL_SMTP_TLS, DEFAULT_EMAIL_SMTP_TLS)));
        emailSender.setSmtpAuth(Boolean.parseBoolean(getConfigValue(line, props, EMAIL_SMTP_AUTH, DEFAULT_EMAIL_SMTP_AUTH)));
        emailSender.setEmailFrom(getConfigValue(line, props, EMAIL_FROM));
        emailSender.setSmtpPassword(getConfigValue(line, props, EMAIL_SMTP_PASSWORD));
        emailSender.init();
    }
	
    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }
    
	public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        Configuration properties = readProperties();
        SchemaChangeWatcher watcher = new SchemaChangeWatcher(properties);
        
    	String[] clusterUriKeys = getConfigValues(line, properties, CONFIG_URI_KEYS);
    	if (clusterUriKeys.length > 0) {
    		int keyNum = 0;
    		String[] clusterUris = new String[clusterUriKeys.length];
    		for (String clusterUriKey : clusterUriKeys) {
    			clusterUris[keyNum++] = getConfigValue(line, properties, clusterUriKey);
        	}
    		watcher.setClusterUris(clusterUris);
    	} else {
    		watcher.setClusterUris(getConfigValues(line, properties, CLUSTER_URI));
    	}
        watcher.setClusterUriKeys(clusterUriKeys);
        
        String[] clusterNames = getConfigValues(line, properties, CLUSTER_NAMES);
        watcher.setClusterNames(clusterNames);
        watcher.initEmailReportConfig(line, properties);
        
        String checkIntervalStr = getConfigValue(line, properties, CHECK_INTERVAL_SECONDS, "60");
        watcher.setCheckIntervalSeconds(Long.parseLong(checkIntervalStr));
        
        boolean watchCollectionUuids = Boolean.parseBoolean(getConfigValue(line, properties, WATCH_COLLECTION_UUIDS, "false"));
        watcher.setWatchCollectionUuids(watchCollectionUuids);
        
        watcher.init();
        
    }


	public void setClusterUris(String[] clusterUris) {
		this.clusterUris = clusterUris;
	}


	public void setClusterUriKeys(String[] clusterUriKeys) {
		this.clusterUriKeys = clusterUriKeys;
	}


	public void setClusterNames(String[] clusterNames) {
		this.clusterNames = clusterNames;
	}


	public void setCheckIntervalSeconds(long checkIntervalSeconds) {
		this.checkIntervalSeconds = checkIntervalSeconds;
	}

	public void setWatchCollectionUuids(boolean watchCollectionUuids) {
		this.watchCollectionUuids = watchCollectionUuids;
	}



}
