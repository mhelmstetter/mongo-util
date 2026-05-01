package com.mongodb.mongostat;

import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
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

    @Option(names = {"--index-details"}, description = "Show individual indexes as separate rows (disabled by default)")
    private boolean indexDetails = false;

    @Option(names = {"-i", "--interval"}, description = "Interval between stats collection in seconds", defaultValue = "15")
    private long intervalSecs = 15;

    @Option(names = {"--cache-size-gb"}, description = "Manually specify WiredTiger cache size in GB (bypasses serverStatus)")
    private Double cacheSizeGB;

    @Option(names = {"-v", "--verbose"}, description = "Enable verbose/debug logging")
    private boolean verbose = false;

    @Option(names = {"--sort"}, description = "Sort collections by: cacheMB (default), dirtyMB, dataGB, idxGB, namespace, dirty%%, idxDty%%, readMB, writMB", defaultValue = "cacheMB")
    private String sortBy = "cacheMB";

    @Option(names = {"--top"}, description = "Display only top N collections/indexes (0 = show all)", defaultValue = "0")
    private int top = 0;

    @Option(names = {"--shardPivot"}, description = "Pivot table view with shards as columns")
    private boolean shardPivot = false;

    @Option(names = {"--pivotMetrics"}, description = "Metrics to show in pivot view (comma-separated: readMB,writMB,cacheMB,dirtyMB)", defaultValue = "readMB,writMB")
    private String pivotMetrics = "readMB,writMB";

    @Option(names = {"--cache-mb"}, description = "Show cacheMB column (current bytes in WT cache per collection)")
    private boolean includeCacheMb = false;

    @Option(names = {"--dirty-mb"}, description = "Show dirtyMB column (dirty bytes in WT cache per collection)")
    private boolean includeDirtyMb = false;

    @Option(names = {"--no-color"}, description = "Disable ANSI color highlighting in pivot view")
    private boolean noColor = false;

    @Option(names = {"--cumulative"}, description = "Show cumulative totals since server startup (readGB/writGB) instead of per-interval deltas")
    private boolean cumulativeMode = false;

    @Override
    public Integer call() throws Exception {
        // Set logging level based on verbose flag
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger mongostatLogger = loggerContext.getLogger("com.mongodb.mongostat");
        mongostatLogger.setLevel(verbose ? Level.DEBUG : Level.INFO);

        // Also control ShardClient logging (used for connecting to shards)
        ch.qos.logback.classic.Logger shardClientLogger = loggerContext.getLogger("com.mongodb.shardsync.ShardClient");
        shardClientLogger.setLevel(verbose ? Level.DEBUG : Level.INFO);

        // --cumulative defaults pivot to raw GB totals unless user overrode --pivotMetrics.
        // readGBh/writGBh (uptime-normalized) are available explicitly via --pivotMetrics.
        if (cumulativeMode && "readMB,writMB".equals(pivotMetrics)) {
            pivotMetrics = "readGB,writGB";
        }

        MongoStatConfiguration config = new MongoStatConfiguration()
                .jsonOutput(jsonOutput)
                .includeWiredTigerStats(!disableWiredTiger)  // Default enabled, disable with --no-wt
                .includeCollectionStats(!disableCollections)  // Default enabled, disable with --no-coll
                .detailedOutput(!disableDetail)
                .includeIndexDetails(indexDetails)  // Default disabled, enable with --index-details
                .intervalMs(intervalSecs * 1000)
                .sortBy(sortBy)
                .top(top)
                .shardPivot(shardPivot)
                .pivotMetrics(pivotMetrics)
                .includeCacheMb(includeCacheMb)
                .includeDirtyMb(includeDirtyMb)
                .noColor(noColor)
                .cumulativeMode(cumulativeMode);

        // If cache size is manually specified, convert GB to bytes
        if (cacheSizeGB != null) {
            long cacheSizeBytes = (long)(cacheSizeGB * 1024 * 1024 * 1024);
            config.manualCacheSizeBytes(cacheSizeBytes);
        }

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
