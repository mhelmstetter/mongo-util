package com.mongodb.corruptutil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class TransactionTest {
    
    private static Logger logger = LoggerFactory.getLogger(TransactionTest.class);
    
    private static Options options;
    
    private MongoClient sourceClient;
    private int threads = 100;
    private boolean useTransactions;
    
    private String dbName;
    
    private ExecutorService executor;
    
    public TransactionTest(String sourceUriStr) {
    	ConnectionString connectionString = new ConnectionString(sourceUriStr);
    	MongoClientSettings mongoClientSettings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .build();
    	sourceClient = MongoClients.create(mongoClientSettings);
    }
    
    public void run() throws InterruptedException {
        executor = Executors.newFixedThreadPool(threads);
        
        for (int i = 0; i < threads; i++) {
        	Runnable worker = new TransactionWorker(sourceClient, i, useTransactions, dbName);
            executor.execute(worker);
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
            Thread.sleep(1000);
        }
        logger.debug("CorruptUtil complete");
    }
    
    private void setThreads(int threads) {
        this.threads = threads;
    }

    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArgs().withLongOpt("source")
                .isRequired(true).create("s"));
        
        options.addOption(OptionBuilder.withArgName("database name").hasArgs().withLongOpt("db")
                .isRequired(true).create("d"));
        
        options.addOption(OptionBuilder.withArgName("use transactions")
                .withLongOpt("withTransaction").create("x"));
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
        formatter.printHelp("logParser", options);
        System.exit(-1);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);
        TransactionTest util = new TransactionTest(line.getOptionValue("s"));
        String threadsStr = line.getOptionValue("t");
        if (threadsStr != null) {
            int threads = Integer.parseInt(threadsStr);
            util.setThreads(threads);
        }
        
        boolean useTransactions = line.hasOption("x");
        util.setUseTransactions(useTransactions);
        
        String dbName = line.getOptionValue("d");
        util.setDbName(dbName);
        
        util.run();

    }

	public boolean isUseTransactions() {
		return useTransactions;
	}

	public void setUseTransactions(boolean useTransactions) {
		this.useTransactions = useTransactions;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}



}
