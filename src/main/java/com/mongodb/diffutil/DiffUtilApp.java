package com.mongodb.diffutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import com.mongodb.shardsync.ShardConfigSyncApp;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DiffUtilApp {
	
	private static Logger logger = LoggerFactory.getLogger(DiffUtilApp.class);
    
    private static Options options;
    private static CommandLine line;
    
    private final static String SOURCE_URI = "source";
    private final static String DEST_URI = "dest";

    private final static String COLL_COUNTS = "compareCounts";
    //private final static String CHUNK_COUNTS = "chunkCounts";
    private final static String COMPARE_DOCUMENTS = "compareDocuments";
    private final static String COMPARE_DOCUMENTS_QUERY = "compareDocumentsQuery";
    
    private final static String COMPARE_IDS = "compareIds";
    private final static String RETRY = "retry";
    
    private final static String NO_REPORT_MISSING = "noReportMissing";
    private final static String REPORT_MATCHES = "reportMatches";
    private final static String SAMPLE_RATE = "sampleRate";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source").create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArgs().withLongOpt("dest").create("d"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArgs().withLongOpt("config")
                .isRequired(false).create("c"));
        options.addOption(OptionBuilder.withArgName("Include namespace").hasArgs().withLongOpt("includeNamespace").create("f"));
        options.addOption(OptionBuilder.withArgName("Namespace mapping").hasArgs().withLongOpt("namespaceMap").create("m"));
        options.addOption(OptionBuilder.withArgName("Compare counts only")
                .withLongOpt(COLL_COUNTS).create(COLL_COUNTS));
        options.addOption(OptionBuilder.withArgName("Compare all documents in all collections, using parallel cursors")
                .withLongOpt(COMPARE_DOCUMENTS).create(COMPARE_DOCUMENTS));
        options.addOption(OptionBuilder.withArgName("Compare all documents in all collections, using per document query match")
                .withLongOpt(COMPARE_DOCUMENTS_QUERY).create(COMPARE_DOCUMENTS_QUERY));
        options.addOption(OptionBuilder.withArgName("Compare ids ")
                .withLongOpt(COMPARE_IDS).create(COMPARE_IDS));
        options.addOption(OptionBuilder.withArgName("Retry failed")
                .withLongOpt(RETRY).create(RETRY));
        options.addOption(OptionBuilder.withArgName("Do not report missing docs")
                .withLongOpt(NO_REPORT_MISSING).create(NO_REPORT_MISSING));
        options.addOption(OptionBuilder.withArgName("Report missing docs")
                .withLongOpt(REPORT_MATCHES).create(REPORT_MATCHES));
        options.addOption(OptionBuilder.withArgName("Sample rate")
                .withLongOpt(SAMPLE_RATE).hasArg().create());
        

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
    
    private static Properties readProperties() {
        Properties prop = new Properties();
        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else  {
            propsFile = new File("diff-util.properties");
            if (! propsFile.exists()) {
            	propsFile = new File(ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE);
            }
            
            if (! propsFile.exists()) {
                logger.warn("Default config files diff-util.properties or {}, not found, using command line options only", ShardConfigSyncApp.SHARD_SYNC_PROPERTIES_FILE);
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

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        Properties configFileProps = readProperties();
        DiffUtil sync = new DiffUtil(configFileProps);
        sync.setSourceClusterUri(line.getOptionValue("s", configFileProps.getProperty(SOURCE_URI)));
        sync.setDestClusterUri(line.getOptionValue("d", configFileProps.getProperty(DEST_URI)));
        
        if (line.hasOption(REPORT_MATCHES)) {
        	sync.setReportMatches(true);
        }
        if (line.hasOption(NO_REPORT_MISSING)) {
        	sync.setReportMissing(false);
        }
        
        if (line.hasOption(SAMPLE_RATE)) {
        	String sampleRateStr = line.getOptionValue(SAMPLE_RATE);
        	sync.setGlobalSampleRate(Double.parseDouble(sampleRateStr));
        }
        
        String[] mappings = line.getOptionValues("m");
        sync.setMappings(mappings);
        
        String[] includes = line.getOptionValues("f"); 
        sync.setIncludes(includes);
        
        sync.init();
        if (line.hasOption(COLL_COUNTS)) {
            sync.compareShardCounts();
        }
        if (line.hasOption(COMPARE_DOCUMENTS)) {
            sync.compareDocuments(true);
        }
        if (line.hasOption(COMPARE_DOCUMENTS_QUERY)) {
            sync.compareDocuments(false);
        }
        if (line.hasOption(COMPARE_IDS)) {
        	if (mappings != null && mappings.length > 0) {
        		sync.compareIdsMapped();
        	} else {
        		sync.compareIds();
        	}
            
        }
        if (line.hasOption(RETRY)) {
        	sync.retry();
        }
        
        

    }

}
