package com.mongodb.shardsync;

import static com.mongodb.shardsync.BaseConfiguration.Constants.DEST_URI;
import static com.mongodb.shardsync.BaseConfiguration.Constants.SOURCE_URI;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.command.*;
import com.mongodb.shardsync.command.AdvancedConnectionMixin;
import com.mongodb.shardsync.command.compare.CompareCommand;
import com.mongodb.shardsync.command.drop.DropCommand;
import com.mongodb.shardsync.command.sync.SyncCommand;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.ParseResult;
import picocli.CommandLine.PropertiesDefaultProvider;

@Command(name = "shardSync", 
         mixinStandardHelpOptions = true, 
         description = "MongoDB shard synchronization utility",
         defaultValueProvider = PropertiesDefaultProvider.class,
         subcommands = {
             SyncCommand.class,
             CompareCommand.class, 
             MongomirrorCommand.class,
             ShardingCommand.class,
             DropCommand.class
         })
public class ShardConfigSyncApp implements Callable<Integer> {
    
    public static final String SHARD_SYNC_PROPERTIES_FILE = "shard-sync.properties";
    
    private int exitCode = 0;
    private static Logger logger = LoggerFactory.getLogger(ShardConfigSyncApp.class);
    
    // Global options shared across all sub-commands
    @Option(names = {"-c", "--config"}, description = "Configuration properties file", defaultValue = SHARD_SYNC_PROPERTIES_FILE)
    private File configFile;

    @Option(names = {"-s", "--" + SOURCE_URI}, description = "Source cluster connection uri")
    private String sourceUri;

    @Option(names = {"-d", "--" + DEST_URI}, description = "Destination cluster connection uri")
    private String destUri;


    @Option(names = {"-m", "--shardMap"}, arity = "0..*", description = "Shard mapping, format: <sourceShard>|<destShard>,...\n" +
    		"example: shard1|atlas-xd8hfg-shard-0,shard10|atlas-xd8hfg-shard-10\n" +
    		"This will by default map the shards in db order,\n" + 
    		"however if single digit numbers are used, the\n" + 
    		"source and target numbers may not align and\n" +
    		"shardMap is recommended for consistency")
    private String[] shardMap;

    @Option(names = {"-f", "--filter"}, arity = "0..*", description = "Namespace filter")
    private String[] filter;

    @Option(names = {"--nonPrivileged"}, description = "Non-privileged mode, create chunks using splitChunk")
    private boolean nonPrivileged;

    @Option(names = {"--sslAllowInvalidHostnames"}, description = "ssl allow invalid hostnames (not implemented)", hidden = true)
    private boolean sslAllowInvalidHostnames;

    @Option(names = {"--sslAllowInvalidCertificates"}, description = "ssl allow invalid certificates (not implemented)", hidden = true)
    private boolean sslAllowInvalidCerts;

    @Option(names = {"--dryRun"}, description = "Dry run only")
    private boolean dryRun;

    // Advanced connection options grouped in a mixin
    @CommandLine.Mixin
    private AdvancedConnectionMixin advancedConnection = new AdvancedConnectionMixin();

    // Keep only the constants that are used by other classes
    public final static String SOURCE_RS_MANUAL = "sourceRsManual";
    public final static String DEST_RS_MANUAL = "destRsManual";
    public final static String NON_PRIVILEGED = "nonPrivileged";

    @Override
    public Integer call() throws Exception {
        CommandLine commandLine = new CommandLine(this);
        String subcommands = String.join(", ", commandLine.getSubcommands().keySet());
        System.out.println("Please specify a sub-command: " + subcommands);
        System.out.println("Use --help to see available options");
        return 1;
    }

    // Default values for email configuration
    private static final String DEFAULT_EMAIL_SMTP_HOST = "smtp.gmail.com";
    private static final String DEFAULT_EMAIL_SMTP_PORT = "587";
    private static final String DEFAULT_EMAIL_SMTP_TLS = "true";
    private static final String DEFAULT_EMAIL_SMTP_AUTH = "true";
    private static final String DEFAULT_ERROR_MESSAGE_WINDOW_SECS = "300";
    private static final String DEFAULT_ERROR_REPORT_MAX = "25";
    private static final String DEFAULT_EMAIL_REPORT_MAX = "9999";

    public SyncConfiguration createBaseConfiguration() {
        return createBaseConfiguration(null);
    }
    
    public SyncConfiguration createBaseConfiguration(AtlasOptionsMixin atlasOptions) {
        SyncConfiguration config = new SyncConfiguration();
        config.setSourceClusterUri(sourceUri);
        config.setDestClusterUri(destUri);
        
        // Use Atlas options from mixin if provided, otherwise fall back to global options
        if (atlasOptions != null) {
            config.setAtlasApiPublicKey(atlasOptions.atlasApiPublicKey);
            config.setAtlasApiPrivateKey(atlasOptions.atlasApiPrivateKey);
            config.setAtlasProjectId(atlasOptions.atlasProjectId);
        }

        // Set advanced connection options
        config.setSourceClusterPattern(advancedConnection.sourceUriPattern);
        config.setDestClusterPattern(advancedConnection.destUriPattern);
        config.setSourceRsSsl(advancedConnection.sourceRsSsl);
        config.setSourceRsManual(advancedConnection.sourceRsManual);
        config.setDestRsManual(advancedConnection.destRsManual);
        config.setSourceRsRegex(advancedConnection.sourceRsRegex);
        config.setDestRsRegex(advancedConnection.destRsRegex);
        config.setDestCsrsUri(advancedConnection.destCsrsUri);
        config.setSourceRsPattern(advancedConnection.sourceRsPattern);
        config.setDestRsPattern(advancedConnection.destRsPattern);

        if ((config.getSourceClusterUri() == null && config.getSourceClusterPattern() == null)
                || (config.getDestClusterUri() == null && config.getDestClusterPattern() == null)) {
            System.out.println("sourceUri/destUri and/or sourceUriPattern/destUriPattern options required");
            System.exit(-1);
        }

        if (shardMap != null) {
            config.setShardMap(shardMap);
        }

        if (filter != null) {
            config.setNamespaceFilters(filter);
        }

        config.setNonPrivilegedMode(nonPrivileged);
        config.setDryRun(dryRun);

        config.setSslAllowInvalidCertificates(sslAllowInvalidCerts);
        config.setSslAllowInvalidHostnames(sslAllowInvalidHostnames);

        return config;
    }


    public Integer executeCompareCommand(CompareCommand compareCmd) throws Exception {
        SyncConfiguration config = createBaseConfiguration(compareCmd.getAtlasOptions());

        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (compareCmd.isCollCounts()) {
            sync.compareShardCounts();
        } else if (compareCmd.isChunkCounts()) {
            sync.compareChunkCounts();
        } else if (compareCmd.isCompareChunks()) {
            sync.compareChunks();
        } else if (compareCmd.isCompareChunksEquivalent()) {
            sync.compareChunksEquivalent();
        } else if (compareCmd.isCompareAndMoveChunks()) {
            sync.compareAndMoveChunks(true, false);
        } else if (compareCmd.isCompareCollectionUuids()) {
            sync.compareCollectionUuids();
        } else if (compareCmd.isCompareDbMeta()) {
            sync.compareDatabaseMetadata();
        } else if (compareCmd.isCompareIndexes()) {
            exitCode = sync.compareIndexes(false);
        } else if (compareCmd.isDiffUsers()) {
            sync.diffUsers();
        } else if (compareCmd.isDiffRoles()) {
            sync.diffRoles();
        } else if (compareCmd.getDiffShardKeys() != null) {
            boolean doSync = compareCmd.getDiffShardKeys().equals("sync");
            sync.diffShardedCollections(doSync);
        } else {
            System.out.println("No compare action specified. Use --help to see available options.");
            return 1;
        }

        return exitCode;
    }

    public Integer executeMongomirrorCommand(MongomirrorCommand mongoCmd) throws Exception {
        SyncConfiguration config = createBaseConfiguration();
        
        String mongoMirrorPath = mongoCmd.getMongomirrorBinary();

        if (mongoCmd.getCollStatsThreshold() != null) {
            Integer collStatsThresholdInt = Integer.parseInt(mongoCmd.getCollStatsThreshold());
            config.setCollStatsThreshold(collStatsThresholdInt);
        }

        if (mongoCmd.getCompressors() != null) {
            config.setCompressors(mongoCmd.getCompressors());
        }
        if (mongoCmd.getMongoMirrorStartPort() != null) {
            Integer startPort = Integer.parseInt(mongoCmd.getMongoMirrorStartPort());
            config.setMongoMirrorStartPort(startPort);
        }
        config.setOplogBasePath(mongoCmd.getOplogBasePath());
        config.setBookmarkFilePrefix(mongoCmd.getBookmarkFilePrefix());

        if (mongoMirrorPath == null) {
            System.out.println("mongomirrorBinary required");
            return 1;
        }
        config.setMongomirrorBinary(mongoMirrorPath);
        config.setNoIndexRestore(mongoCmd.isNoIndexRestore());
        config.setSleepMillis(mongoCmd.getSleepMillis());
        config.setNumParallelCollections(mongoCmd.getNumParallelCollections());
        config.setWriteConcern(mongoCmd.getWriteConcern());
        
        if (mongoCmd.getStopWhenLagWithin() != null) {
            config.setStopWhenLagWithin(Integer.parseInt(mongoCmd.getStopWhenLagWithin()));
        }

        initMongoMirrorEmailReportConfig(config, mongoCmd);

        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();

        if (mongoCmd.isShardToRs()) {
            logger.debug("shardToRs");
            config.setMongomirrorBinary(mongoMirrorPath);
            sync.shardToRs();
        } else if (mongoCmd.isTailFromNow()) {
            sync.mongomirrorTailFromNow();
        } else if (mongoCmd.isTailFromLatestOplogTs()) {
            sync.mongomirrorTailFromLatestOplogTs(null);
        } else if (mongoCmd.getTailFromTs() != null) {
            sync.mongomirrorTailFromTs(mongoCmd.getTailFromTs());
        } else {
            sync.mongomirror();
        }

        return exitCode;
    }

    public Integer executeShardingCommand(ShardingCommand shardingCmd) throws Exception {
        SyncConfiguration config = createBaseConfiguration();
        
        config.setCleanupOrphansSleepMillis(shardingCmd.getCleanupOrphansSleep());

        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (shardingCmd.isFlushRouter()) {
            sync.flushRouterConfig();
        } else if (shardingCmd.isDisableSourceAutosplit()) {
            sync.disableSourceAutosplit();
        } else if (shardingCmd.isShardCollections()) {
            sync.shardCollections();
        } else if (shardingCmd.isCheckShardedIndexes()) {
            sync.checkShardedIndexes();
        } else if (shardingCmd.isCleanupOrphans()) {
            sync.cleanupOrphans();
        } else if (shardingCmd.isCleanupOrphansDest()) {
            sync.cleanupOrphansDest();
        } else {
            System.out.println("No sharding action specified. Use --help to see available options.");
            return 1;
        }

        return exitCode;
    }
    
    public Integer executeDropCommand(DropCommand dropCmd) throws Exception {
        SyncConfiguration config = createBaseConfiguration(dropCmd.getAtlasOptions());
        
        config.setDrop(dropCmd.isDrop());
        config.setDropDestDbs(dropCmd.isDropDestDbs());
        config.setDropDestDbsAndConfigMetadata(dropCmd.isDropDestDbsAndConfigMetadata());

        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();
        
        if (dropCmd.isDropDestDbs()) {
            sync.dropDestinationDatabases();
        } else if (dropCmd.isDropDestDbsAndConfigMetadata()) {
            sync.dropDestinationDatabasesAndConfigMetadata();
        } else if (dropCmd.isDropDestAtlasUsersAndRoles()) {
            sync.dropDestinationAtlasUsersAndRoles();
        } else if (dropCmd.isDropIndexes()) {
            // Drop indexes functionality would be implemented here
            logger.warn("Drop indexes functionality not yet implemented");
        } else {
            System.out.println("No drop action specified. Use --help to see available options.");
            return 1;
        }

        return exitCode;
    }

    private void initMongoMirrorEmailReportConfig(SyncConfiguration config, MongomirrorCommand mongoCmd) {
        /* Set email report recipients */
        /* Expecting a comma-delimited string here; split it into a list */
        List<String> emailReportRecipients = new ArrayList<>();
        String emailReportRecipientsRaw = mongoCmd.getEmailRecipients();
        if (emailReportRecipientsRaw == null) {
        	logger.debug("emailRecipients not configured, skipping mongomirror email reporting config");
        	return;
        }
        String[] rawSplits = emailReportRecipientsRaw.split(",");
        for (String raw : rawSplits) {
            if (raw.length() > 0) {
                emailReportRecipients.add(raw.trim());
            }
        }
        config.setEmailReportRecipients(emailReportRecipients);

        String smtpHost = mongoCmd.getEmailSmtpHost() != null ? mongoCmd.getEmailSmtpHost() : DEFAULT_EMAIL_SMTP_HOST;
        config.setSmtpHost(smtpHost);

        int smtpPort = Integer.parseInt(mongoCmd.getEmailSmtpPort() != null ? mongoCmd.getEmailSmtpPort() : DEFAULT_EMAIL_SMTP_PORT);
        config.setSmtpPort(smtpPort);

        boolean smtpTls = Boolean.parseBoolean(mongoCmd.getEmailSmtpTls() != null ? mongoCmd.getEmailSmtpTls() : DEFAULT_EMAIL_SMTP_TLS);
        config.setSmtpStartTlsEnable(smtpTls);

        boolean smtpAuth = Boolean.parseBoolean(mongoCmd.getEmailSmtpAuth() != null ? mongoCmd.getEmailSmtpAuth() : DEFAULT_EMAIL_SMTP_AUTH);
        config.setSmtpAuth(smtpAuth);

        String mailFrom = mongoCmd.getEmailFrom();
        config.setMailFrom(mailFrom);

        String smtpPassword = mongoCmd.getEmailSmtpPassword();
        config.setSmtpPassword(smtpPassword);

        int errorMessageWindowSecsInt = Integer.parseInt(mongoCmd.getErrorMessageWindowSecs() != null ? mongoCmd.getErrorMessageWindowSecs() : DEFAULT_ERROR_MESSAGE_WINDOW_SECS);
        config.setErrorMessageWindowSecs(errorMessageWindowSecsInt);

        int errorReportMaxInt = Integer.parseInt(mongoCmd.getErrorReportMax() != null ? mongoCmd.getErrorReportMax() : DEFAULT_ERROR_REPORT_MAX);
        config.setErrorReportMax(errorReportMaxInt);

        int emailReportMaxInt = Integer.parseInt(mongoCmd.getEmailReportMax() != null ? mongoCmd.getEmailReportMax() : DEFAULT_EMAIL_REPORT_MAX);
        config.setEmailReportMax(emailReportMaxInt);
    }

    public static void main(String[] args) throws Exception {
        ShardConfigSyncApp app = new ShardConfigSyncApp();

        try {
            // First CommandLine instance: parse to extract config file
            CommandLine tempCmd = new CommandLine(new ShardConfigSyncApp());
            ParseResult tempResult = tempCmd.parseArgs(args);

            File defaultsFile;
            ShardConfigSyncApp tempApp = tempResult.commandSpec().commandLine().getCommand();
            if (tempApp.configFile != null) {
                defaultsFile = tempApp.configFile;
            } else {
                defaultsFile = new File(SHARD_SYNC_PROPERTIES_FILE);
            }

            // Second CommandLine instance: parse with defaults loaded
            CommandLine cmd = new CommandLine(app);
            if (defaultsFile.exists()) {
                cmd.setDefaultValueProvider(new PropertiesDefaultProvider(defaultsFile));
            }
            
            ParseResult parseResult = cmd.parseArgs(args);

            if (!CommandLine.printHelpIfRequested(parseResult)) {
                int exitCode = cmd.execute(args);
                System.exit(exitCode);
            }
        } catch (ParameterException ex) {
            System.err.println(ex.getMessage());
            ex.getCommandLine().usage(System.err);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Error executing ShardConfigSyncApp", e);
            System.exit(1);
        }
    }
}