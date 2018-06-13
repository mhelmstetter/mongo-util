package com.mongodb.diffutil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

public class DiffUtilApp {
    
    private static Options options;

    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    //private final static String COMPARE_CHUNKS = "compareChunks";
    
    private final static String COMPARE_IDS = "compareIds";

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source")
                .isRequired(true).create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArgs().withLongOpt("dest")
                .isRequired(true).create("d"));
        options.addOption(OptionBuilder.withArgName("Compare counts only")
                .withLongOpt(COLL_COUNTS).create(COLL_COUNTS));
        options.addOption(OptionBuilder.withArgName("Compare chunk counts")
                .withLongOpt(CHUNK_COUNTS).create(CHUNK_COUNTS));
        options.addOption(OptionBuilder.withArgName("Compare ids ")
                .withLongOpt(COMPARE_IDS).create(COMPARE_IDS));

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
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        DiffUtil sync = new DiffUtil();
        sync.setSourceClusterUri(line.getOptionValue("s"));
        sync.setDestClusterUri(line.getOptionValue("d"));
        sync.init();
        if (line.hasOption(COLL_COUNTS)) {
            //sync.compareShardCounts();
        }
        if (line.hasOption(CHUNK_COUNTS)) {
            sync.compareChunks();
        }
        if (line.hasOption(COMPARE_IDS)) {
            sync.compareIds();
        }
        
        // String[] fileNames = line.getOptionValues("f");
        // client.setEndpointUrl(line.getOptionValue("u"));

    }

}
