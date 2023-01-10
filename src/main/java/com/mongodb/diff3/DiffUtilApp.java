package com.mongodb.diff3;

import static com.mongodb.diff3.DiffConfiguration.PARTITION_MODE;
import static com.mongodb.diff3.DiffConfiguration.SHARD_MODE;
import static com.mongodb.diff3.DiffConfiguration.RECHECK_MODE;
import static org.apache.commons.cli.OptionBuilder.withArgName;

import java.io.File;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.diff3.partition.PartitionDiffUtil;
import com.mongodb.diff3.shard.ShardDiffUtil;

public class DiffUtilApp {

    private static final Logger logger = LoggerFactory.getLogger(DiffUtilApp.class);

    private static CommandLine line;

    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    private final static String THREADS = "threads";
    private final static String SAMPLE_RATE = "sampleRate";
    private final static String SAMPLE_MIN_DOCS = "sampleMinDocs";
    private final static String MAX_DOCS_TO_SAMPLE_PER_PARTITION = "maxDocsToSamplePerPartition";
    private final static String DEFAULT_PARTITION_SIZE = "defaultPartitionSize";
    private final static String MODE = "mode";
    private final static String DEFAULT_MODE = "partition";
    private final static String MAX_RETRIES = "maxRetries";
    private final static String USE_STATUS_DB = "useStatusDb";
    private final static String STATUS_DB_URI = "statusDbUri";
    private final static String STATUS_DB_NAME = "statusDbName";
    private final static String STATUS_DB_COLL_NAME = "statusDbCollName";
    private static final String SHARD_MAP = "shardMap";

    private final static String DEFAULT_THREADS = "8";
    private final static String DEFAULT_SAMPLE_RATE = "0.04";
    private final static String DEFAULT_SAMPLE_MIN_DOCS = "101";
    private final static String DEFAULT_MAX_DOCS_TO_SAMPLE_PER_PARTITION = "10";
    private final static String DEFAULT_DEFAULT_PARTITION_SIZE = String.valueOf(400  * 1024 * 1024);
    private final static String DEFAULT_MAX_RETRIES = "5";
    private final static String DEFAULT_USE_STATUS_DB = "true";
    private final static String DEFAULT_STATUS_DB_NAME = "Diff3";
    private final static String DEFAULT_STATUS_DB_COLL_NAME = "Status";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(withArgName("Source cluster connection uri").hasArg()
                .withLongOpt(SOURCE_URI).create("s"));
        options.addOption(withArgName("Destination cluster connection uri").hasArg()
                .withLongOpt(DEST_URI).create("d"));
        options.addOption(withArgName("Configuration properties file").hasArg()
                .withLongOpt("config").isRequired(false).create("c"));
        options.addOption(withArgName("Include namespace").hasArgs()
                .withLongOpt("includeNamespace").create("f"));

        options.addOption(withArgName("Number of worker threads").hasArg()
                .withLongOpt(THREADS).create("t"));
        options.addOption(withArgName("Mode (one of: [shard, partition {default}])").hasArg()
                .withLongOpt("mode").create("m"));
        options.addOption(withArgName("Sample rate for partitions").hasArg()
                .withLongOpt(SAMPLE_RATE).create());
        options.addOption(withArgName("Min docs to sample for partitions").hasArg()
                .withLongOpt(SAMPLE_MIN_DOCS).create());
        options.addOption(withArgName("Max docs to sample per partition").hasArg()
                .withLongOpt(MAX_DOCS_TO_SAMPLE_PER_PARTITION).create());
        options.addOption(withArgName("Default size (bytes) for partitions").hasArg()
                .withLongOpt(DEFAULT_PARTITION_SIZE).create());
        options.addOption(withArgName("Max retries").hasArg().withLongOpt(MAX_RETRIES).create());
        options.addOption(withArgName("Use Status DB").hasArg().withLongOpt(USE_STATUS_DB).create());
        options.addOption(withArgName("Status DB URI").hasArg().withLongOpt(STATUS_DB_URI).create());
        options.addOption(withArgName("Status DB Name").hasArg().withLongOpt(STATUS_DB_NAME).create());
        options.addOption(withArgName("Status DB Collection Name").hasArg().withLongOpt(STATUS_DB_COLL_NAME).create());

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

    private static void printHelpAndExit(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    private static Configuration readProperties() {
        Configurations configs = new Configurations();
        Configuration defaultConfig = new PropertiesConfiguration();

        File propsFile;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else {
            propsFile = new File("diff-util.properties");
            if (!propsFile.exists()) {
                propsFile = new File("shard-sync.properties");
            }
            if (!propsFile.exists()) {
                logger.warn("Default config files diff-util.properties or shard-sync.properties, not found, using command line options only");
                return defaultConfig;
            }
        }

        try {
            return configs.properties(propsFile);
        } catch (ConfigurationException e) {
            logger.error("Error loading properties file: " + propsFile, e);
        }
        return defaultConfig;
    }

    private static String getConfigValue(CommandLine line, Configuration props, String key, String defaultValue) {
        return defaultValue != null && defaultValue.length() > 0 ?
                line.getOptionValue(key, props.getString(key, defaultValue)) :
                line.getOptionValue(key, props.getString(key));
    }

    private static String getConfigValue(CommandLine line, Configuration props, String key) {
        return getConfigValue(line, props, key, null);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);

        Configuration properties = readProperties();

        DiffConfiguration config = new DiffConfiguration();

        config.setSourceClusterUri(getConfigValue(line, properties, SOURCE_URI));
        config.setDestClusterUri(getConfigValue(line, properties, DEST_URI));
        int threads = Integer.parseInt(getConfigValue(line, properties, THREADS, DEFAULT_THREADS));
        config.setThreads(threads);
        config.setMode(getConfigValue(line, properties, MODE, DEFAULT_MODE).trim().toLowerCase());

        config.setSampleRate(Double.parseDouble(
                getConfigValue(line, properties, SAMPLE_RATE, DEFAULT_SAMPLE_RATE)));
        config.setSampleMinDocs(Integer.parseInt(
                getConfigValue(line, properties, SAMPLE_MIN_DOCS, DEFAULT_SAMPLE_MIN_DOCS)));
        config.setMaxDocsToSamplePerPartition(Integer.parseInt(
                getConfigValue(line, properties, MAX_DOCS_TO_SAMPLE_PER_PARTITION,
                        DEFAULT_MAX_DOCS_TO_SAMPLE_PER_PARTITION)));
        config.setDefaultPartitionSize(Long.parseLong(
                getConfigValue(line, properties, DEFAULT_PARTITION_SIZE, DEFAULT_DEFAULT_PARTITION_SIZE)));
        config.setMaxRetries(Integer.parseInt(getConfigValue(line, properties, MAX_RETRIES, DEFAULT_MAX_RETRIES)));
        config.setUseStatusDb(Boolean.parseBoolean(getConfigValue(
                line, properties, USE_STATUS_DB, DEFAULT_USE_STATUS_DB)));
        if (config.isUseStatusDb()) {
            config.setStatusDbUri(getConfigValue(line, properties, STATUS_DB_URI));
            config.setStatusDbName(getConfigValue(line, properties, STATUS_DB_NAME, DEFAULT_STATUS_DB_NAME));
            config.setStatusDbCollName(getConfigValue(line, properties,
                    STATUS_DB_COLL_NAME, DEFAULT_STATUS_DB_COLL_NAME));
        }
        
        String shardMaps = properties.getString(SHARD_MAP);
        if (shardMaps != null) {
            config.setShardMap(shardMaps.split(","));
        } else {
            config.setShardMap(line.getOptionValues("m"));
        }

        config.setNamespaceFilters(line.getOptionValues("f"));
        if (config.getMode().equals(PARTITION_MODE)) {
            PartitionDiffUtil diffUtil = new PartitionDiffUtil(config);
            diffUtil.run();
        } else if (config.getMode().equals(SHARD_MODE)) {
            ShardDiffUtil shardDiffUtil = new ShardDiffUtil(config);
            shardDiffUtil.run();
            
        } else if (config.getMode().equals(RECHECK_MODE)) {
        	RecheckUtil rechecker = new RecheckUtil(config);
        	rechecker.recheck();
        	
        } else {
            System.out.println("Unknown mode: " + config.getMode() + ". Exiting.");
            System.exit(1);
        }

    }

}
