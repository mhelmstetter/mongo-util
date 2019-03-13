package com.mongodb.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class ConnStorm {
    
    private String mongoUriStr;
    private int threads;
    
    private List<ConnStormRunnable> runners;
    
    private Boolean storm = false;
    
    public void execute() {
        
        MongoClientURI uri = new MongoClientURI(mongoUriStr);
        MongoClient mongoClient = new MongoClient(uri);
        mongoClient.getDatabase("admin").runCommand(new Document("ping",1));
        
        runners = new ArrayList<ConnStormRunnable>(threads);
        
        for (int i = 0; i < threads; i++) {
            ConnStormRunnable runnable = new ConnStormRunnable(uri, this);
            //Thread thread = new Thread(runnable, "ConnStormRunnable_" + i);
            runnable.start();
            runners.add(runnable);
        }
        mongoClient.close();
        
        while (true) {
            System.out.println("Storm");
            storm = true;
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
            }
            synchronized(this) {
                this.notifyAll();
            }
            
            
            try {
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
            }
        }
        
    }
    
    @SuppressWarnings("static-access")
    protected static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        Options options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(
                OptionBuilder.withArgName("target mongo uri").hasArg().withLongOpt("host").isRequired().create("h"));

        options.addOption(OptionBuilder.withArgName("# threads").hasArgs().withLongOpt("threads").create("t"));
        
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
        formatter.printHelp("replayUtil", options);
        System.exit(-1);
    }

    protected void parseArgs(String args[]) {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        this.mongoUriStr = line.getOptionValue("h");

        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            this.threads = Integer.parseInt(threadsStr);
        }
    }
    
    public static void main(String args[]) throws Exception {

        ConnStorm stormy = new ConnStorm();
        stormy.parseArgs(args);
        stormy.execute();
    }

}
