package com.mongodb.diffutil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

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
    
    private final static String COMPARE_IDS = "compareIds";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source").create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArgs().withLongOpt("dest").create("d"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArgs().withLongOpt("config")
                .isRequired(false).create("c"));
        options.addOption(OptionBuilder.withArgName("Include namespace").hasArgs().withLongOpt("includeNamespace").create("f"));
        options.addOption(OptionBuilder.withArgName("Compare counts only")
                .withLongOpt(COLL_COUNTS).create(COLL_COUNTS));
        options.addOption(OptionBuilder.withArgName("Compare all documents in all collections")
                .withLongOpt(COMPARE_DOCUMENTS).create(COMPARE_DOCUMENTS));
        options.addOption(OptionBuilder.withArgName("Compare ids ")
                .withLongOpt(COMPARE_IDS).create(COMPARE_IDS));

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
            	propsFile = new File("shard-sync.properties");
            }
            
            if (! propsFile.exists()) {
                logger.warn("Default config files diff-util.properties or shard-sync.properties, not found, using command line options only");
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
        DiffUtil sync = new DiffUtil();
        sync.setSourceClusterUri(line.getOptionValue("s", configFileProps.getProperty(SOURCE_URI)));
        sync.setDestClusterUri(line.getOptionValue("d", configFileProps.getProperty(DEST_URI)));
        
        String[] includes = line.getOptionValues("f"); 
        sync.setIncludes(includes);
        
        sync.init();
        if (line.hasOption(COLL_COUNTS)) {
            sync.compareShardCounts();
        }
        if (line.hasOption(COMPARE_DOCUMENTS)) {
            sync.compareDocuments();
        }
        if (line.hasOption(COMPARE_IDS)) {
            sync.compareIds();
        }
        
        

    }

}
