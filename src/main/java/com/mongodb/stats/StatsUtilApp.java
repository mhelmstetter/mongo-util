package com.mongodb.stats;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class StatsUtilApp {
    
    private static Options options;

    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        
        options.addOption(new Option("help", "print this message"));
        
        options.addOption(Option.builder().longOpt("uri").desc("MongoDB connection uri").hasArg().build());
        
        CommandLineParser parser = new DefaultParser();
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
        sync.setMongoUri(line.getOptionValue("uri"));
        sync.init();
        sync.stats();
    }

}
