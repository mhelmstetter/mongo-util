package com.mongodb.oplog.analyzer;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Shard;
import com.mongodb.shardsync.ShardClient;
import com.opencsv.CSVWriter;

public class OplogAnalyzer {

	protected static final Logger logger = LoggerFactory.getLogger(OplogAnalyzer.class);

	private static Options options;

	private String sourceUri;
	private ShardClient sourceShardClient;
	private ExecutorService executor;
	private List<OplogAnalyzerWorker> workers;

	public void init() {
		sourceShardClient = new ShardClient("source", sourceUri);
		sourceShardClient.init();
		sourceShardClient.populateShardMongoClients();
	}

	public void run() {
		Map<String, Shard> shardsMap = sourceShardClient.getShardsMap();
		int numThreads = shardsMap.size();
		workers = new ArrayList<>(numThreads);
		executor = Executors.newFixedThreadPool(numThreads);

		for (String shardId : shardsMap.keySet()) {
			OplogAnalyzerWorker worker = new OplogAnalyzerWorker(sourceShardClient, shardId);
			workers.add(worker);
			executor.execute(worker);
		}
	}

	@SuppressWarnings("static-access")
	private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
		options = new Options();
		options.addOption(new Option("help", "print this message"));
		options.addOption(OptionBuilder.withArgName("mongodb connection uri").hasArg().withLongOpt("uri")
				.isRequired(true).create("u"));

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

	private void shutdown() {
		logger.debug("starting shutdown");

		if (executor != null) {
			for (OplogAnalyzerWorker w : workers) {
				w.stop();
			}
		}

		executor.shutdownNow();
		try {
			executor.awaitTermination(5, TimeUnit.MINUTES);
		} catch (InterruptedException e) {
			//logger.warn("interrupted");
			Thread.currentThread().interrupt();
		}
		logger.debug("shutdown complete");
	}

	private static void addShutdownHook(OplogAnalyzer analyzer) {
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			public void run() {
				logger.debug("**** SHUTDOWN *****");
				analyzer.shutdown();
			}
		}));
	}

	private static void printHelpAndExit(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("logParser", options);
		System.exit(-1);
	}

	public static void main(String[] args) throws Exception {
		CommandLine line = initializeAndParseCommandLineOptions(args);

		OplogAnalyzer analyzer = new OplogAnalyzer();
		analyzer.setSourceUri(line.getOptionValue("u"));
		analyzer.init();
		analyzer.run();
		addShutdownHook(analyzer);
	}

	public void setSourceUri(String sourceUri) {
		this.sourceUri = sourceUri;
	}

}
