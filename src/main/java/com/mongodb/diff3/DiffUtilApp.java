package com.mongodb.diff3;

import java.io.File;

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

import static org.apache.commons.cli.OptionBuilder.withArgName;

public class DiffUtilApp {

    private static Logger logger = LoggerFactory.getLogger(DiffUtilApp.class);

    private static Options options;
    private static CommandLine line;

    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";
    private final static String THREADS = "threads";
    private final static String SAMPLE_RATE = "sampleRate";
    private final static String SAMPLE_MIN_DOCS = "sampleMinDocs";
    private final static String MAX_DOCS_TO_SAMPLE_PER_PARTITION = "maxDocsToSamplePerPartition";
    private final static String DEFAULT_PARTITION_SIZE = "defaultPartitionSize";

    private final static String DEFAULT_THREADS = "8";
    private final static String DEFAULT_SAMPLE_RATE = "0.04";
    private final static String DEFAULT_SAMPLE_MIN_DOCS = "101";
    private final static String DEFAULT_MAX_DOCS_TO_SAMPLE_PER_PARTITION = "10";
    private final static String DEFAULT_DEFAULT_PARTITION_SIZE = String.valueOf(400 * 1024 * 1024);

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
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
        options.addOption(withArgName("Sample rate for partitions").hasArg()
                .withLongOpt(SAMPLE_RATE).create());
        options.addOption(withArgName("Min docs to sample for partitions").hasArg()
                .withLongOpt(SAMPLE_MIN_DOCS).create());
        options.addOption(withArgName("Max docs to sample per partition").hasArg()
                .withLongOpt(MAX_DOCS_TO_SAMPLE_PER_PARTITION).create());
        options.addOption(withArgName("Default size (bytes) for partitions").hasArg()
                .withLongOpt(DEFAULT_PARTITION_SIZE).create());

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

        File propsFile = null;
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
            Configuration config = configs.properties(propsFile);
            return config;
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
        boolean partitionMode = false;
        if (args.length > 0) {
            String mode = args[0].replaceAll("[-_]*", "").trim().toUpperCase();
            if (mode.equals("PARTITIONMODE")) {
                partitionMode = true;
            }
        }
        CommandLine line = initializeAndParseCommandLineOptions(args);

        Configuration properties = readProperties();

        DiffConfiguration config = new DiffConfiguration();

        config.setSourceClusterUri(getConfigValue(line, properties, SOURCE_URI));
        config.setDestClusterUri(getConfigValue(line, properties, DEST_URI));
        int threads = Integer.parseInt(getConfigValue(line, properties, THREADS, DEFAULT_THREADS));
        config.setThreads(threads);

        config.setSampleRate(Double.parseDouble(
                getConfigValue(line, properties, SAMPLE_RATE, DEFAULT_SAMPLE_RATE)));
        config.setSampleMinDocs(Integer.parseInt(
                getConfigValue(line, properties, SAMPLE_MIN_DOCS, DEFAULT_SAMPLE_MIN_DOCS)));
        config.setMaxDocsToSamplePerPartition(Integer.parseInt(
                getConfigValue(line, properties, MAX_DOCS_TO_SAMPLE_PER_PARTITION,
                        DEFAULT_MAX_DOCS_TO_SAMPLE_PER_PARTITION)));
        config.setDefaultPartitionSize(Long.parseLong(
                getConfigValue(line, properties, DEFAULT_PARTITION_SIZE, DEFAULT_DEFAULT_PARTITION_SIZE)));

        config.setNamespaceFilters(line.getOptionValues("f"));
        if (partitionMode) {
            DifferentDiffUtil diffUtil = new DifferentDiffUtil(config);
            diffUtil.run();
        } else {
            DiffUtil diffUtil = new DiffUtil(config);
            diffUtil.run();
        }

    }

}
