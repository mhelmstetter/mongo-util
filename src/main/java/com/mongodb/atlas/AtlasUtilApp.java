package com.mongodb.atlas;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class AtlasUtilApp {
    
    private static Options options;

    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    //private final static String COMPARE_CHUNKS = "compareChunks";
    
    private final static String COMPARE_IDS = "compareIds";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("API user").hasArgs().withLongOpt("user")
                .isRequired(true).create("u"));
        options.addOption(OptionBuilder.withArgName("API key").hasArgs().withLongOpt("key")
                .isRequired(true).create("k"));
        options.addOption(OptionBuilder.withArgName("Group Id").hasArgs().withLongOpt("groupId")
                .isRequired(true).create("g"));

        

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
        
        AtlasUtil util = new AtlasUtil(line.getOptionValue("u"), line.getOptionValue("k"), "mongodb://localhost:27017/atlasMetrics");
        

        util.getClusters(line.getOptionValue("g"));
    }

}
