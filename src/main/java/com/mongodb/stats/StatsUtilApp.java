package com.mongodb.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class StatsUtilApp {
    
    private static Options options;

    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    //private final static String COMPARE_CHUNKS = "compareChunks";
    
    private final static String COMPARE_IDS = "compareIds";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("MongoDB connection uri").hasArgs().withLongOpt("uri")
                .isRequired(true).create("u"));
        options.addOption(OptionBuilder.withArgName("Database name").hasArgs().withLongOpt("database")
                .isRequired(true).create("d"));
        options.addOption(OptionBuilder.withArgName("Collection name").hasArgs().withLongOpt("collection")
                .isRequired(true).create("c"));
        options.addOption(OptionBuilder.withArgName("Group field").hasArgs().withLongOpt("field")
                .isRequired(true).create("f"));
        

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
        formatter.printHelp("statsUtil", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        StatsUtil sync = new StatsUtil();
        sync.setMongoUri(line.getOptionValue("u"));
        sync.setDatabase(line.getOptionValue("d"));
        sync.setCollection(line.getOptionValue("c"));
        sync.setGroupField(line.getOptionValue("f"));
        sync.init();
        sync.stats();
    }

}
