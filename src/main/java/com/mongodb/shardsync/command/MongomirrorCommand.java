package com.mongodb.shardsync.command;

import com.mongodb.shardsync.ShardConfigSyncApp;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.Callable;

@Command(name = "mongomirror", 
         mixinStandardHelpOptions = true,
         description = "Run mongomirror operations")
public class MongomirrorCommand implements Callable<Integer> {
    
    @CommandLine.ParentCommand
    private ShardConfigSyncApp parent;
    
    @Option(names = {"--mongomirrorBinary"}, description = "Path to mongomirror binary (default: mongomirror)", defaultValue = "mongomirror")
    private String mongomirrorBinary;
    
    @Option(names = {"--shardToRs"}, description = "Migrate from sharded cluster to replica set")
    private boolean shardToRs;
    
    @Option(names = {"--tailFromNow"}, description = "Start tailing oplog from current time")
    private boolean tailFromNow;
    
    @Option(names = {"--tailFromLatestOplogTs"}, description = "Start tailing from latest oplog timestamp")
    private boolean tailFromLatestOplogTs;
    
    @Option(names = {"--tailFromTs"}, description = "Start tailing from specific timestamp")
    private String tailFromTs;
    
    @Option(names = {"--collStatsThreshold"}, description = "Collection stats threshold")
    private String collStatsThreshold;
    
    @Option(names = {"--compressors"}, description = "Compression algorithms to use")
    private String compressors;
    
    @Option(names = {"--mongoMirrorStartPort"}, description = "Starting port for mongomirror instances (default: 9001)")
    private String mongoMirrorStartPort;
    
    @Option(names = {"--oplogBasePath"}, description = "Base path for oplog files")
    private String oplogBasePath;
    
    @Option(names = {"--bookmarkFilePrefix"}, description = "Prefix for bookmark files")
    private String bookmarkFilePrefix;
    
    @Option(names = {"--noIndexRestore"}, description = "Skip index restoration")
    private boolean noIndexRestore;
    
    @Option(names = {"--dryRun"}, description = "Dry run only")
    private boolean dryRun;
    
    @Option(names = {"--sleepMillis"}, description = "Sleep duration in milliseconds between mongomirror process launches (ramp up) (default: 0)")
    private Integer sleepMillis = 0;
    
    @Option(names = {"--numParallelCollections"}, description = "Number of collections to process in parallel")
    private String numParallelCollections;
    
    @Option(names = {"--writeConcern"}, description = "Write concern for operations")
    private String writeConcern;
    
    @Option(names = {"--stopWhenLagWithin"}, description = "Stop when lag is within specified seconds")
    private String stopWhenLagWithin;
    
    // Email configuration options
    @Option(names = {"--emailRecipients"}, description = "Comma-separated list of email recipients")
    private String emailRecipients;
    
    @Option(names = {"--emailSmtpHost"}, description = "SMTP host for email")
    private String emailSmtpHost;
    
    @Option(names = {"--emailSmtpPort"}, description = "SMTP port for email")
    private String emailSmtpPort;
    
    @Option(names = {"--emailSmtpTls"}, description = "Enable TLS for SMTP")
    private String emailSmtpTls;
    
    @Option(names = {"--emailSmtpAuth"}, description = "Enable SMTP authentication")
    private String emailSmtpAuth;
    
    @Option(names = {"--emailFrom"}, description = "From address for emails")
    private String emailFrom;
    
    @Option(names = {"--emailSmtpPassword"}, description = "SMTP password")
    private String emailSmtpPassword;
    
    @Option(names = {"--errorMessageWindowSecs"}, description = "Error message window in seconds")
    private String errorMessageWindowSecs;
    
    @Option(names = {"--errorReportMax"}, description = "Maximum number of errors to report")
    private String errorReportMax;
    
    @Option(names = {"--emailReportMax"}, description = "Maximum number of email reports")
    private String emailReportMax;
    
    @Option(names = {"--verbose"}, arity = "0..1", fallbackValue = "3", description = "Set verbose level (0-5, default: 3 when option is specified)")
    private Integer verbose;
    
    @Option(names = {"--mongomirrorLogPath"}, description = "Directory path for mongomirror log files (default: current directory)")
    private String mongomirrorLogPath;

    @Option(names = {"--pprof"}, description = "Enable pprof profiling on specified address:port (e.g., localhost:6060)")
    private String pprof;

    @Override
    public Integer call() throws Exception {
        return parent.executeMongomirrorCommand(this);
    }
    
    // Getters for the parent class to access the fields
    public String getMongomirrorBinary() { return mongomirrorBinary; }
    public boolean isShardToRs() { return shardToRs; }
    public boolean isTailFromNow() { return tailFromNow; }
    public boolean isTailFromLatestOplogTs() { return tailFromLatestOplogTs; }
    public String getTailFromTs() { return tailFromTs; }
    public String getCollStatsThreshold() { return collStatsThreshold; }
    public String getCompressors() { return compressors; }
    public String getMongoMirrorStartPort() { return mongoMirrorStartPort; }
    public String getOplogBasePath() { return oplogBasePath; }
    public String getBookmarkFilePrefix() { return bookmarkFilePrefix; }
    public boolean isNoIndexRestore() { return noIndexRestore; }
    public boolean isDryRun() { return dryRun; }
    public Integer getSleepMillis() { return sleepMillis; }
    public String getNumParallelCollections() { return numParallelCollections; }
    public String getWriteConcern() { return writeConcern; }
    public String getStopWhenLagWithin() { return stopWhenLagWithin; }
    public String getEmailRecipients() { return emailRecipients; }
    public String getEmailSmtpHost() { return emailSmtpHost; }
    public String getEmailSmtpPort() { return emailSmtpPort; }
    public String getEmailSmtpTls() { return emailSmtpTls; }
    public String getEmailSmtpAuth() { return emailSmtpAuth; }
    public String getEmailFrom() { return emailFrom; }
    public String getEmailSmtpPassword() { return emailSmtpPassword; }
    public String getErrorMessageWindowSecs() { return errorMessageWindowSecs; }
    public String getErrorReportMax() { return errorReportMax; }
    public String getEmailReportMax() { return emailReportMax; }
    public Integer getVerbose() { return verbose; }
    public String getMongomirrorLogPath() { return mongomirrorLogPath; }
    public String getPprof() { return pprof; }
}