package com.mongodb.mongostat;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "mongostat", 
         mixinStandardHelpOptions = true,
         description = "Enhanced MongoDB statistics monitor with WiredTiger cache and collection stats",
         version = "1.1.0")
public class MongoStatApp implements Callable<Integer> {
    
    private static Logger logger = LoggerFactory.getLogger(MongoStatApp.class);
    
    @Parameters(index = "0", description = "MongoDB connection URI", arity = "1")
    private String uri;
    
    @Option(names = {"-j", "--json"}, description = "Output in JSON format")
    private boolean jsonOutput = false;
    
    @Option(names = {"--no-wt"}, description = "Disable WiredTiger cache statistics (enabled by default)")
    private boolean disableWiredTiger = false;
    
    @Option(names = {"--no-coll"}, description = "Disable collection statistics (enabled by default)")
    private boolean disableCollections = false;
    
    @Option(names = {"--no-detail"}, description = "Disable detailed per-collection stats (enabled by default)")
    private boolean disableDetail = false;
    
    @Option(names = {"-i", "--interval"}, description = "Interval between stats collection in seconds", defaultValue = "15")
    private long intervalSecs = 15;

    @Override
    public Integer call() throws Exception {
        MongoStatConfiguration config = new MongoStatConfiguration()
                .jsonOutput(jsonOutput)
                .includeWiredTigerStats(!disableWiredTiger)  // Default enabled, disable with --no-wt
                .includeCollectionStats(!disableCollections)  // Default enabled, disable with --no-coll
                .detailedOutput(!disableDetail)
                .intervalMs(intervalSecs * 1000);
        
        MongoStat mongoStat = MongoStat.create(uri, config);
        mongoStat.init();
        mongoStat.run();
        
        return 0;
    }
    
    public static void main(String[] args) {
        int exitCode = new CommandLine(new MongoStatApp()).execute(args);
        System.exit(exitCode);
    }

}
