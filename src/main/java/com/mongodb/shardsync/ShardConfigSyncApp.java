package com.mongodb.shardsync;

import static com.mongodb.shardsync.BaseConfiguration.Constants.*;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.fluent.Configurations;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShardConfigSyncApp {

    private static Logger logger = LoggerFactory.getLogger(ShardConfigSyncApp.class);

    private static Options options;
    private static CommandLine line;

    private final static String SOURCE_URI_PATTERN = "sourceUriPattern";
    private final static String DEST_URI_PATTERN = "destUriPattern";
    private final static String DEST_CSRS_URI = "destCsrs";

    private final static String SOURCE_RS_PATTERN = "sourceRsPattern";
    private final static String DEST_RS_PATTERN = "destRsPattern";

    // Advanced option for manaually overriding the hostnames / bypassing discovery
    public final static String SOURCE_RS_MANUAL = "sourceRsManual";
    public final static String DEST_RS_MANUAL = "destRsManual";
    
    // for the case where mongos uses ssl but shards to not
    private final static String SOURCE_RS_SSL = "sourceRsSsl";
    
    private final static String SOURCE_RS_REGEX = "sourceRsRegex";
    private final static String DEST_RS_REGEX = "destRsRegex";

    private final static String MONGOMIRROR_BINARY = "mongomirrorBinary";

    private final static String FILTER = "filter";
    private final static String DROP = "drop";
    private final static String DROP_DEST_DBS = "dropDestDbs";
    private final static String DROP_DEST_DBS_AND_CONFIG_METADATA = "dropDestDbsAndConfigMeta";
    public final static String NON_PRIVILEGED = "nonPrivileged";
    private final static String PRESERVE_UUIDS = "preserveUUIDs";
    private final static String NO_INDEX_RESTORE = "noIndexRestore";
    private final static String COMPRESSORS = "compressors";
    private final static String COLL_STATS_THRESHOLD = "collStatsThreshold";

    private final static String COLL_COUNTS = "compareCounts";
    private final static String CHUNK_COUNTS = "chunkCounts";
    private final static String FLUSH_ROUTER = "flushRouter";
    private final static String SYNC_METADATA = "syncMetadata";
    private final static String SYNC_METADATA_OPTIMIZED = "syncMetadataOptimized";
    private final static String SYNC_USERS = "syncUsers";
    private final static String USERS_INPUT_CSV = "usersInput";
    private final static String USERS_OUTPUT_CSV = "usersOutput";
    private final static String SYNC_ROLES = "syncRoles";
    private final static String DIFF_USERS = "diffUsers";
    private final static String DIFF_ROLES = "diffRoles";
    private final static String TEST_USERS_AUTH = "testUsersAuth";
    private final static String COMPARE_CHUNKS = "compareChunks";
    private final static String COMPARE_CHUNKS_EQUIVALENT = "compareChunksEquivalent";
    private final static String COMPARE_COLLECTION_UUIDS = "compareCollectionUuids";
    private final static String COMPARE_DB_META = "compareDbMeta";
    private final static String DISABLE_SOURCE_AUTOSPLIT = "disableSourceAutosplit";
    private final static String MONGOMIRROR_START_PORT = "mongoMirrorStartPort";
    private final static String OPLOG_BASE_PATH = "oplogBasePath";
    private final static String BOOKMARK_FILE_PREFIX = "bookmarkFilePrefix";
    private final static String SKIP_FLUSH_ROUTER_CONFIG = "skipFlushRouterConfig";
    private final static String DROP_DEST_ATLAS_USERS_AND_ROLES = "dropDestAtlasUsersAndRoles";
    private final static String COMPARE_AND_MOVE_CHUNKS = "compareAndMoveChunks";
    private final static String MONGO_MIRROR = "mongomirror";
    private final static String DRY_RUN = "dryRun";
    private final static String TAIL_FROM_TS = "tailFromTs";
    private final static String TAIL_FROM_NOW = "tailFromNow";
    private final static String TAIL_FROM_LATEST_OPLOG_TS = "tailFromLatestOplogTs";
    private final static String SHARD_COLLECTIONS = "shardCollections";
    private final static String CLEANUP_ORPHANS = "cleanupOrphans";
    private final static String CLEANUP_ORPHANS_SLEEP = "cleanupOrphansSleep";
    private final static String CLEANUP_ORPHANS_DEST = "cleanupOrphansDest";
    private final static String SYNC_INDEXES = "syncIndexes";
    private final static String COLL_MOD_TTL = "collModTtl";
    private final static String COMPARE_INDEXES = "compareIndexes";
    private final static String CHECK_SHARDED_INDEXES = "checkShardedIndexes";
    private final static String EXTEND_TTL = "extendTtl";
    private final static String CLEANUP_PREVIOUS_ALL = "cleanupPreviousAll";
    private final static String CLEANUP_PREVIOUS_SHARDS = "cleanupPreviousShards";
    private final static String COLLATION = "collation";
    private final static String DROP_INDEXES = "dropIndexes";

    private final static String SSL_ALLOW_INVALID_HOSTNAMES = "sslAllowInvalidHostnames";
    private final static String SSL_ALLOW_INVALID_CERTS = "sslAllowInvalidCertificates";

    private static final String SHARD_MAP = "shardMap";

    private static final String ATLAS_API_PUBLIC_KEY = "atlasApiPublicKey";
    private static final String ATLAS_API_PRIVATE_KEY = "atlasApiPrivateKey";
    private static final String ATLAS_PROJECT_ID = "atlasProjectId";
    private static final String EMAIL_RECIPIENTS = "emailReportRecipients";
    private static final String EMAIL_SMTP_HOST = "emailSmtpHost";
    private static final String EMAIL_SMTP_PORT = "emailSmtpPort";
    private static final String EMAIL_SMTP_TLS = "emailSmtpTls";
    private static final String EMAIL_SMTP_AUTH = "emailSmtpAuth";
    private static final String EMAIL_FROM = "emailFrom";
    private static final String EMAIL_SMTP_PASSWORD = "emailSmtpPassword";
    private static final String ERROR_MESSAGE_WINDOW_SECS = "errorMessageWindowSecs";
    private static final String ERROR_REPORT_MAX = "errorReportMax";
    private static final String EMAIL_REPORT_MAX = "emailReportMax";
    private static final String STOP_WHEN_LAG_WITHIN = "stopWhenLagWithin";
    private static final String DEFAULT_EMAIL_SMTP_HOST = "smtp.gmail.com";
    private static final String DEFAULT_EMAIL_SMTP_PORT = "587";
    private static final String DEFAULT_EMAIL_SMTP_TLS = "true";
    private static final String DEFAULT_EMAIL_SMTP_AUTH = "true";
    private static final String DEFAULT_ERROR_MESSAGE_WINDOW_SECS = "300";
    private static final String DEFAULT_ERROR_REPORT_MAX = "25";
    private static final String DEFAULT_EMAIL_REPORT_MAX = "9999";


    @SuppressWarnings("static-access")
    private static CommandLine initializeAndParseCommandLineOptions(String[] args) {
        options = new Options();
        options.addOption(new Option("help", "print this message"));
        options.addOption(OptionBuilder.withArgName("Configuration properties file").hasArg().withLongOpt("config")
                .create("c"));
        options.addOption(OptionBuilder.withArgName("Source cluster connection uri").hasArg().withLongOpt(SOURCE_URI)
                .create("s"));
        options.addOption(OptionBuilder.withArgName("Destination cluster connection uri").hasArg().withLongOpt(DEST_URI)
                .create("d"));

        options.addOption(OptionBuilder.withArgName("Drop (context specific)")
                .withLongOpt(DROP).create(DROP));
        options.addOption(OptionBuilder.withArgName("Drop destination databases, but preserve config metadata")
                .withLongOpt(DROP_DEST_DBS).create(DROP_DEST_DBS));
        options.addOption(OptionBuilder.withArgName("Drop destination databases AND config metadata (collections, databases, chunks)")
                .withLongOpt(DROP_DEST_DBS_AND_CONFIG_METADATA).create(DROP_DEST_DBS_AND_CONFIG_METADATA));
        options.addOption(OptionBuilder.withArgName("Drop destination users and roles (via Atlas API)")
                .withLongOpt(DROP_DEST_ATLAS_USERS_AND_ROLES).create(DROP_DEST_ATLAS_USERS_AND_ROLES));
        options.addOption(OptionBuilder.withArgName("Non-privileged mode, create chunks using splitChunk")
                .withLongOpt(NON_PRIVILEGED).create(NON_PRIVILEGED));
        options.addOption(OptionBuilder.withArgName("Compare counts only (do not sync/migrate)")
                .withLongOpt(COLL_COUNTS).create(COLL_COUNTS));
        options.addOption(OptionBuilder.withArgName("Show chunk counts when collection counts differ")
                .withLongOpt(CHUNK_COUNTS).create(CHUNK_COUNTS));
        options.addOption(OptionBuilder.withArgName("Flush router config on all mongos (do not sync/migrate)")
                .withLongOpt(FLUSH_ROUTER).create(FLUSH_ROUTER));
        options.addOption(OptionBuilder.withArgName("Compare all shard chunks (do not sync/migrate)")
                .withLongOpt(COMPARE_CHUNKS).create(COMPARE_CHUNKS));
        options.addOption(OptionBuilder.withArgName("Compare all shard chunks for equivalency")
                .withLongOpt(COMPARE_CHUNKS_EQUIVALENT).create(COMPARE_CHUNKS_EQUIVALENT));
        options.addOption(OptionBuilder.withArgName("Compare all shard chunks, move any misplaced chunks")
                .withLongOpt(COMPARE_AND_MOVE_CHUNKS).create(COMPARE_AND_MOVE_CHUNKS));
        options.addOption(OptionBuilder.withArgName("Compare all collection UUIDs")
                .withLongOpt(COMPARE_COLLECTION_UUIDS).create(COMPARE_COLLECTION_UUIDS));
        options.addOption(OptionBuilder.withArgName("Compare database metadata")
                .withLongOpt(COMPARE_DB_META).create(COMPARE_DB_META));
        options.addOption(OptionBuilder.withArgName("Cleanup (remove) destination data on all shards")
                .withLongOpt(CLEANUP_PREVIOUS_ALL).create(CLEANUP_PREVIOUS_ALL));
        options.addOption(OptionBuilder.withArgName("Cleanup (remove) destination data on specified shards").hasArgs()
                .withLongOpt(CLEANUP_PREVIOUS_SHARDS).create(CLEANUP_PREVIOUS_SHARDS));

        options.addOption(OptionBuilder.withArgName("Synchronize shard metadata")
                .withLongOpt(SYNC_METADATA).create(SYNC_METADATA));
        options.addOption(OptionBuilder.withArgName("Synchronize shard metadata (optimized method)")
                .withLongOpt(SYNC_METADATA_OPTIMIZED).create(SYNC_METADATA_OPTIMIZED));
        options.addOption(OptionBuilder.withArgName("Synchronize users to Atlas")
                .withLongOpt(SYNC_USERS).create(SYNC_USERS));
        options.addOption(OptionBuilder.withArgName("Synchronize roles to Atlas")
                .withLongOpt(SYNC_ROLES).create(SYNC_ROLES));
        options.addOption(OptionBuilder.withArgName("Diff users with Atlas")
                .withLongOpt(DIFF_USERS).create(DIFF_USERS));
        options.addOption(OptionBuilder.withArgName("Diff roles with Atlas")
                .withLongOpt(DIFF_ROLES).create(DIFF_ROLES));
        options.addOption(OptionBuilder.withArgName("Test user authentications using credentials from input csv")
                .withLongOpt(TEST_USERS_AUTH).create(TEST_USERS_AUTH));
        options.addOption(OptionBuilder.withArgName("users input csv").hasArg()
                .withLongOpt(USERS_INPUT_CSV).create(USERS_INPUT_CSV));
        
        options.addOption(OptionBuilder.withArgName("Shard mapping").hasArg().withLongOpt(SHARD_MAP)
                .isRequired(false).create("m"));
        options.addOption(OptionBuilder.withArgName("Disable autosplit on source cluster")
                .withLongOpt(DISABLE_SOURCE_AUTOSPLIT).create());
        options.addOption(OptionBuilder.withArgName("Cleanup source orphans")
                .withLongOpt(CLEANUP_ORPHANS).create(CLEANUP_ORPHANS));
        options.addOption(OptionBuilder.withArgName("Cleanup destination orphans")
                .withLongOpt(CLEANUP_ORPHANS_DEST).create(CLEANUP_ORPHANS_DEST));
        options.addOption(OptionBuilder.withArgName("cleanup orphans sleep millis").hasArg()
                .withLongOpt(CLEANUP_ORPHANS_SLEEP).create(CLEANUP_ORPHANS_SLEEP));

        options.addOption(OptionBuilder.withArgName("Shard destination collections")
                .withLongOpt(SHARD_COLLECTIONS).create());
        options.addOption(OptionBuilder.withArgName("Drop dest indexes")
                .withLongOpt(DROP_INDEXES).create(DROP_INDEXES));
        options.addOption(OptionBuilder.withArgName("Copy indexes from source to dest")
                .withLongOpt(SYNC_INDEXES).create(SYNC_INDEXES));
        options.addOption(OptionBuilder.withArgName("Collation to apply to index creation")
                .withLongOpt(COLLATION).create(COLLATION));
        options.addOption(OptionBuilder.withArgName("collMod all ttl indexes on dest based on source")
                .withLongOpt(COLL_MOD_TTL).create(COLL_MOD_TTL));
        options.addOption(OptionBuilder.withArgName("Compare indexes from source to dest")
                .withLongOpt(COMPARE_INDEXES).create(COMPARE_INDEXES));
        options.addOption(OptionBuilder.withArgName("Check sharded indexes")
                .withLongOpt(CHECK_SHARDED_INDEXES).create(CHECK_SHARDED_INDEXES));
        options.addOption(OptionBuilder.withArgName("Extend TTL expiration (use with syncIndexes)")
                .withLongOpt(EXTEND_TTL).create(EXTEND_TTL));
        options.addOption(OptionBuilder.withArgName("Skip the flushRouterConfig step")
                .withLongOpt(SKIP_FLUSH_ROUTER_CONFIG).create(SKIP_FLUSH_ROUTER_CONFIG));

        options.addOption(OptionBuilder.withArgName("ssl allow invalid hostnames")
                .withLongOpt(SSL_ALLOW_INVALID_HOSTNAMES).create(SSL_ALLOW_INVALID_HOSTNAMES));
        options.addOption(OptionBuilder.withArgName("ssl allow invalid certificates")
                .withLongOpt(SSL_ALLOW_INVALID_CERTS).create(SSL_ALLOW_INVALID_CERTS));


        // Mongomirror options
        options.addOption(OptionBuilder.withArgName("Execute mongomirror(s)")
                .withLongOpt(MONGO_MIRROR).create(MONGO_MIRROR));
        options.addOption(OptionBuilder.withArgName("Dry run only")
                .withLongOpt(DRY_RUN).create(DRY_RUN));
        options.addOption(OptionBuilder.withArgName("mongomirror tail only starting at current ts")
                .withLongOpt(TAIL_FROM_NOW).create(TAIL_FROM_NOW));
        options.addOption(OptionBuilder.withArgName("mongomirror tail only starting from latest oplog ts")
                .withLongOpt(TAIL_FROM_LATEST_OPLOG_TS).create(TAIL_FROM_LATEST_OPLOG_TS));
        options.addOption(OptionBuilder.withArgName("mongomirror tail only from specificed oplog ts (ts,increment format)")
                .withLongOpt(TAIL_FROM_TS).hasArg().create(TAIL_FROM_TS));
        options.addOption(OptionBuilder.withArgName("mongomirror namespace filter").hasArgs()
                .withLongOpt(FILTER).create("f"));
        options.addOption(OptionBuilder.withArgName("full path to mongomirror binary").hasArg()
                .withLongOpt(MONGOMIRROR_BINARY).create("p"));
        options.addOption(OptionBuilder.withArgName("mongomirror preserve dest UUIDs (not supported for Atlas dest)")
                .withLongOpt(PRESERVE_UUIDS).create(PRESERVE_UUIDS));
        options.addOption(OptionBuilder.withArgName("skip build indexes")
                .withLongOpt(NO_INDEX_RESTORE).create(NO_INDEX_RESTORE));
        options.addOption(OptionBuilder.withArgName("mongomirror compressors").hasArg()
                .withLongOpt(COMPRESSORS).create(COMPRESSORS));
        options.addOption(OptionBuilder.withArgName("mongomirror collStatsThreshold").hasArg()
                .withLongOpt(COLL_STATS_THRESHOLD).create(COLL_STATS_THRESHOLD));
        options.addOption(OptionBuilder.withArgName("mongomirror http status starting port (default 9001)").hasArg()
                .withLongOpt(MONGOMIRROR_START_PORT).create());
        options.addOption(OptionBuilder.withArgName("mongomirror numParallelCollections").hasArg()
                .withLongOpt("numParallelCollections").create("y"));
        options.addOption(OptionBuilder.withArgName("mongomirror writeConcern").hasArg().withLongOpt("writeConcern")
                .isRequired(false).create("w"));
        options.addOption(OptionBuilder.withArgName("mongomirror oplogPath base path").hasArg()
                .withLongOpt(OPLOG_BASE_PATH).create(OPLOG_BASE_PATH));
        options.addOption(OptionBuilder.withArgName("mongomirror bookmark filename prefix").hasArg()
                .withLongOpt(BOOKMARK_FILE_PREFIX).create());

        options.addOption(OptionBuilder.withArgName("mongomirror email report recipients").hasArg()
                .withLongOpt(EMAIL_RECIPIENTS).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report SMTP host").hasArg()
                .withLongOpt(EMAIL_SMTP_HOST).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report SMTP port").hasArg()
                .withLongOpt(EMAIL_SMTP_PORT).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report TLS enabled").hasArg()
                .withLongOpt(EMAIL_SMTP_TLS).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report auth enabled").hasArg()
                .withLongOpt(EMAIL_SMTP_AUTH).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report sender address").hasArg()
                .withLongOpt(EMAIL_FROM).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report smtp password").hasArg()
                .withLongOpt(EMAIL_SMTP_PASSWORD).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report error message window").hasArg()
                .withLongOpt(ERROR_MESSAGE_WINDOW_SECS).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report max errors per report").hasArg()
                .withLongOpt(ERROR_REPORT_MAX).create());
        options.addOption(OptionBuilder.withArgName("mongomirror email report max reports per run").hasArg()
                .withLongOpt(EMAIL_REPORT_MAX).create());
        options.addOption(OptionBuilder.withArgName("mongomirror stop when lage is below").hasArg()
                .withLongOpt(STOP_WHEN_LAG_WITHIN).create());


        options.addOption(OptionBuilder.withArgName("Sleep millis").hasArg().withLongOpt("sleepMillis")
                .isRequired(false).create("x"));
        options.addOption(OptionBuilder.withArgName("Diff chunks").hasArgs().withLongOpt("diffChunks")
                .isRequired(false).create("z"));

        options.addOption(OptionBuilder.withArgName("shardToRs").withLongOpt("shardToRs")
                .isRequired(false).create("r"));

        options.addOption(OptionBuilder.withArgName("diffShardKeys [sync|diff]").withLongOpt("diffShardKeys")
                .isRequired(false).hasArg().create("k"));


        CommandLineParser parser = new GnuParser();
        try {
            line = parser.parse(options, args);
            if (line.hasOption("help")) {
                printHelpAndExit();
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            printHelpAndExit();
        } catch (Exception e) {
            e.printStackTrace();
            printHelpAndExit();
        }

        return line;
    }

    private static void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("shardSync", options);
        System.exit(-1);
    }

    private static Configuration readProperties() {
        Configurations configs = new Configurations();
        Configuration defaultConfig = new PropertiesConfiguration();

        File propsFile = null;
        if (line.hasOption("c")) {
            propsFile = new File(line.getOptionValue("c"));
        } else {
            propsFile = new File("shard-sync.properties");
            if (!propsFile.exists()) {
                logger.warn("Default config file shard-sync.properties not found, using command line options only");
                return defaultConfig;
            }
        }

        try {
            Configuration config = configs.properties(propsFile);
            return config;
        } catch (ConfigurationException e) {
            logger.error("Error loading properties file: " + propsFile, e);
        }
        return defaultConfig;
    }

    private static String getConfigValue(CommandLine line, Configuration props, String key) {
        return getConfigValue(line, props, key, null);
    }

    private static String getConfigValue(CommandLine line, Configuration props, String key, String defaultValue) {
    	return defaultValue != null && defaultValue.length() > 0 ?
                line.getOptionValue(key, props.getString(key, defaultValue)) :
                line.getOptionValue(key, props.getString(key));
    }

    private static void initMongoMirrorEmailReportConfig(CommandLine line, SyncConfiguration config, Configuration props) {
        /* Set email report recipients */
        /* Expecting a comma-delimited string here; split it into a list */
        List<String> emailReportRecipients = new ArrayList<>();
        String emailReportRecipientsRaw = getConfigValue(line, props, EMAIL_RECIPIENTS);
        if (emailReportRecipientsRaw == null) {
        	logger.debug("{} not configured, skipping mongomirror email reporting config", EMAIL_RECIPIENTS);
        	return;
        }
        String[] rawSplits = emailReportRecipientsRaw.split(",");
        for (String raw : rawSplits) {
            if (raw.length() > 0) {
                emailReportRecipients.add(raw.trim());
            }
        }
        config.setEmailReportRecipients(emailReportRecipients);

        String smtpHost = getConfigValue(line, props, EMAIL_SMTP_HOST, DEFAULT_EMAIL_SMTP_HOST);
        config.setSmtpHost(smtpHost);

        int smtpPort = Integer.parseInt(getConfigValue(line, props, EMAIL_SMTP_PORT, DEFAULT_EMAIL_SMTP_PORT));
        config.setSmtpPort(smtpPort);

        boolean smtpTls = Boolean.parseBoolean(getConfigValue(line, props, EMAIL_SMTP_TLS, DEFAULT_EMAIL_SMTP_TLS));
        config.setSmtpStartTlsEnable(smtpTls);

        boolean smtpAuth = Boolean.parseBoolean(getConfigValue(line, props, EMAIL_SMTP_AUTH, DEFAULT_EMAIL_SMTP_AUTH));
        config.setSmtpAuth(smtpAuth);

        String mailFrom = getConfigValue(line, props, EMAIL_FROM);
        config.setMailFrom(mailFrom);

        String smtpPassword = getConfigValue(line, props, EMAIL_SMTP_PASSWORD);
        config.setSmtpPassword(smtpPassword);

        int errorMessageWindowSecs = Integer.parseInt(getConfigValue(line, props, ERROR_MESSAGE_WINDOW_SECS,
                DEFAULT_ERROR_MESSAGE_WINDOW_SECS));
        config.setErrorMessageWindowSecs(errorMessageWindowSecs);

        int errorReportMax = Integer.parseInt(getConfigValue(line, props, ERROR_REPORT_MAX, DEFAULT_ERROR_REPORT_MAX));
        config.setErrorReportMax(errorReportMax);

        int emailReportMax = Integer.parseInt(getConfigValue(line, props, EMAIL_REPORT_MAX, DEFAULT_EMAIL_REPORT_MAX));
        config.setEmailReportMax(emailReportMax);
    }

    public static void main(String[] args) throws Exception {
        CommandLine line = initializeAndParseCommandLineOptions(args);


        Configuration properties = readProperties();

        SyncConfiguration config = new SyncConfiguration();
        config.setSourceClusterUri(line.getOptionValue("s", properties.getString(SOURCE_URI)));
        config.setDestClusterUri(line.getOptionValue("d", properties.getString(DEST_URI)));

        config.setAtlasApiPublicKey(properties.getString(ATLAS_API_PUBLIC_KEY));
        config.setAtlasApiPrivateKey(properties.getString(ATLAS_API_PRIVATE_KEY));
        config.setAtlasProjectId(properties.getString(ATLAS_PROJECT_ID));
        
        String usersInputCsv = getConfigValue(line, properties, USERS_INPUT_CSV, "usersInput.csv");
        String usersOutputCsv = getConfigValue(line, properties, USERS_OUTPUT_CSV, "usersOutput.csv");
        config.setUsersInputCsv(usersInputCsv);
        config.setUsersOutputCsv(usersOutputCsv);

        config.setSourceClusterPattern(properties.getString(SOURCE_URI_PATTERN));
        config.setDestClusterPattern(properties.getString(DEST_URI_PATTERN));

        config.setSourceRsSsl(properties.getBoolean(SOURCE_RS_SSL, null));
        
        config.setSourceRsManual(properties.getStringArray(SOURCE_RS_MANUAL));
        config.setDestRsManual(properties.getStringArray(DEST_RS_MANUAL));
        
        config.setSourceRsRegex(properties.getString(SOURCE_RS_REGEX));
        config.setDestRsRegex(properties.getString(DEST_RS_REGEX));


        config.setDestCsrsUri(properties.getString(DEST_CSRS_URI));

        config.setSourceRsPattern(properties.getString(SOURCE_RS_PATTERN));
        config.setDestRsPattern(properties.getString(DEST_RS_PATTERN));

        if ((config.getSourceClusterUri() == null && config.getSourceClusterPattern() == null)
                || (config.getDestClusterUri() == null && config.getDestClusterPattern() == null)) {
            System.out.println(String.format("%s/%s and/or %s/%s options required",
                    SOURCE_URI, DEST_URI, SOURCE_URI_PATTERN, DEST_URI_PATTERN));
            printHelpAndExit();
        }

        String shardMaps = properties.getString(SHARD_MAP);
        if (shardMaps != null) {
            config.setShardMap(shardMaps.split(","));
        } else {
            config.setShardMap(line.getOptionValues("m"));
        }

        String[] filter1 = properties.getStringArray(FILTER);
        if (filter1.length > 0) {
        	config.setNamespaceFilters(filter1);
        } else {
        	config.setNamespaceFilters(line.getOptionValues("f"));
        }
        

        boolean nonPrivilegedMode = line.hasOption(NON_PRIVILEGED) || properties.getBoolean(NON_PRIVILEGED, false);
        config.setNonPrivilegedMode(nonPrivilegedMode);
        boolean noIndexRestore = line.hasOption(NO_INDEX_RESTORE) || properties.getBoolean(NO_INDEX_RESTORE, false);
        config.setNoIndexRestore(noIndexRestore);
        config.setDrop(line.hasOption(DROP));
        config.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
        config.setDropDestDbsAndConfigMetadata(line.hasOption(DROP_DEST_DBS_AND_CONFIG_METADATA));
        config.setSleepMillis(line.getOptionValue("x"));
        config.setNumParallelCollections(line.getOptionValue("y"));
        config.setWriteConcern(line.getOptionValue("w"));
        config.setDryRun(line.hasOption(DRY_RUN));
        
        boolean extendTtl = line.hasOption(EXTEND_TTL);
        config.setExtendTtl(extendTtl);

        config.setSslAllowInvalidCertificates(line.hasOption(SSL_ALLOW_INVALID_CERTS));
        config.setSslAllowInvalidHostnames(line.hasOption(SSL_ALLOW_INVALID_HOSTNAMES));

        if (line.hasOption(SKIP_FLUSH_ROUTER_CONFIG) || properties.getBoolean(SKIP_FLUSH_ROUTER_CONFIG, false)) {
            config.setSkipFlushRouterConfig(true);
        }

        if (line.hasOption("r")) {
            config.setShardToRs(true);
        }

        ShardConfigSync sync = new ShardConfigSync(config);
        sync.initialize();

        boolean actionFound = false;
        if (line.hasOption(COLL_COUNTS)) {
            actionFound = true;
            sync.compareShardCounts();
        } else if (line.hasOption(CHUNK_COUNTS)) {
            actionFound = true;
            sync.compareChunkCounts();
        } else if (line.hasOption(FLUSH_ROUTER)) {
            actionFound = true;
            sync.flushRouterConfig();
        } else if (line.hasOption(DISABLE_SOURCE_AUTOSPLIT)) {
            actionFound = true;
            sync.disableSourceAutosplit();
        } else if (line.hasOption(COMPARE_CHUNKS)) {
            actionFound = true;
            sync.compareChunks();
        } else if (line.hasOption(COMPARE_CHUNKS_EQUIVALENT)) {
            actionFound = true;
            sync.compareChunksEquivalent();
        } else if (line.hasOption(COMPARE_AND_MOVE_CHUNKS)) {
            actionFound = true;
            if (nonPrivilegedMode) {
                sync.compareAndMoveChunks(true, false);
            } else {
                //sync.compareAndMovePrivileged();
                sync.compareAndMoveChunks(true, false);
            }

        } else if (line.hasOption(COMPARE_COLLECTION_UUIDS)) {
            actionFound = true;
            sync.compareCollectionUuids();
        } else if (line.hasOption(COMPARE_DB_META)) {
            actionFound = true;
            sync.compareDatabaseMetadata();
        } else if (line.hasOption(SYNC_METADATA)) {
            actionFound = true;
            sync.syncMetadata();
        } else if (line.hasOption(SYNC_METADATA_OPTIMIZED)) {
            actionFound = true;
            sync.syncMetadataOptimized();
        } else if (line.hasOption(SYNC_USERS)) {
            actionFound = true;
            sync.syncUsers();
        } else if (line.hasOption(SYNC_ROLES)) {
            actionFound = true;
            sync.syncRoles();
	    } else if (line.hasOption(DIFF_USERS)) {
	        actionFound = true;
	        sync.diffUsers();
		} else if (line.hasOption(DIFF_ROLES)) {
		    actionFound = true;
		    sync.diffRoles();
		} else if (line.hasOption(TEST_USERS_AUTH)) {
		    actionFound = true;
		    sync.testUsersAuth();
        } else if (line.hasOption(SHARD_COLLECTIONS)) {
            actionFound = true;
            sync.shardCollections();
        } else if (line.hasOption(SYNC_INDEXES)) {
        	String collation = line.getOptionValue(COLLATION, properties.getString(COLLATION));
            actionFound = true;
            sync.syncIndexesShards(true, extendTtl, collation);
        } else if (line.hasOption(COMPARE_INDEXES)) {
            actionFound = true;
            //boolean extendTtl = line.hasOption(EXTEND_TTL);
            //sync.syncIndexesShards(false, extendTtl);
            sync.compareIndexes(false);
        } else if (line.hasOption(CHECK_SHARDED_INDEXES)) {
            actionFound = true;
            sync.checkShardedIndexes();
        } else if (line.hasOption(COLL_MOD_TTL)) {
            actionFound = true;
            sync.compareIndexes(true);
        } else if (line.hasOption("k")) {
            String opt = line.getOptionValue("k");
            boolean doSync = opt.equals("sync");
            actionFound = true;
            sync.diffShardedCollections(doSync);
        } else if (line.hasOption(CLEANUP_ORPHANS)) {
            actionFound = true;
            config.setCleanupOrphansSleepMillis(line.getOptionValue(CLEANUP_ORPHANS_SLEEP));
            sync.cleanupOrphans();
        } else if (line.hasOption(CLEANUP_ORPHANS_DEST)) {
            actionFound = true;
            sync.cleanupOrphansDest();
        } else if (line.hasOption(DROP_DEST_DBS)) {
            actionFound = true;
            sync.dropDestinationDatabases();
        } else if (line.hasOption(DROP_DEST_DBS_AND_CONFIG_METADATA)) {
            actionFound = true;
            sync.dropDestinationDatabasesAndConfigMetadata();
        } else if (line.hasOption(CLEANUP_PREVIOUS_ALL)) {
            actionFound = true;
            sync.cleanupPreviousAll();
        } else if (line.hasOption(CLEANUP_PREVIOUS_SHARDS)) {
        	actionFound = true;
        	String[] shardNames = line.getOptionValues(CLEANUP_PREVIOUS_SHARDS);
        	Set<String> shardNamesSet = new HashSet<>();
        	shardNamesSet.addAll(Arrays.asList(shardNames));
        	sync.cleanupPreviousShards(shardNamesSet);
        } else if (line.hasOption(DROP_DEST_ATLAS_USERS_AND_ROLES)) {
        	actionFound = true;
        	sync.dropDestinationAtlasUsersAndRoles();
        }


        // MONGOMIRROR_BINARY
        if (line.hasOption(MONGO_MIRROR) && !line.hasOption("r")) {
            actionFound = true;
            String mongoMirrorPath = line.getOptionValue("p", properties.getString(MONGOMIRROR_BINARY));
            boolean preserveUUIDs = line.hasOption(PRESERVE_UUIDS);


            if (line.hasOption(COLL_STATS_THRESHOLD)) {
                Integer collStatsThreshold = Integer.parseInt(line.getOptionValue(COLL_STATS_THRESHOLD));
                config.setCollStatsThreshold(collStatsThreshold);
            } else if (properties.getProperty(COLL_STATS_THRESHOLD) != null) {
                config.setCollStatsThreshold(properties.getInt(COLL_STATS_THRESHOLD));
            }

            if (line.hasOption(COMPRESSORS)) {
                config.setCompressors(line.getOptionValue(COMPRESSORS));
            }
            if (line.hasOption(MONGOMIRROR_START_PORT)) {
                Integer startPort = Integer.parseInt(line.getOptionValue(MONGOMIRROR_START_PORT));
                config.setMongoMirrorStartPort(startPort);
            }
            config.setOplogBasePath(line.getOptionValue(OPLOG_BASE_PATH));
            config.setBookmarkFilePrefix(line.getOptionValue(BOOKMARK_FILE_PREFIX));

            if (mongoMirrorPath == null) {
                System.out.println(MONGOMIRROR_BINARY + " required");
                printHelpAndExit();
            }
            config.setMongomirrorBinary(mongoMirrorPath);
            config.setDropDestDbs(line.hasOption(DROP_DEST_DBS));
            config.setPreserveUUIDs(preserveUUIDs);
            config.setNoIndexRestore(noIndexRestore);
            String stopLagWithin = getConfigValue(line, properties, STOP_WHEN_LAG_WITHIN, "");
            if (stopLagWithin != null) {
                config.setStopWhenLagWithin(Integer.parseInt(stopLagWithin));
            }

            initMongoMirrorEmailReportConfig(line, config, properties);

            if (line.hasOption(TAIL_FROM_NOW)) {
                sync.mongomirrorTailFromNow();
            } else if (line.hasOption(TAIL_FROM_LATEST_OPLOG_TS)) {
                sync.mongomirrorTailFromLatestOplogTs(null);
            } else if (line.hasOption(TAIL_FROM_TS)) {
                sync.mongomirrorTailFromTs(line.getOptionValue(TAIL_FROM_TS));
            } else {
                sync.mongomirror();
            }
        }

        if (line.hasOption("r") && line.hasOption(MONGO_MIRROR)) {
            logger.debug("shardToRs");
            actionFound = true;
            String mongoMirrorPath = line.getOptionValue("p", properties.getString(MONGOMIRROR_BINARY));

            initMongoMirrorEmailReportConfig(line, config, properties);

            config.setMongomirrorBinary(mongoMirrorPath);
            config.setDropDestDbs(line.hasOption(DROP_DEST_DBS));

            sync.shardToRs();
        }

        if (!actionFound) {
            System.out.println("Missing action");
            printHelpAndExit();
        }

        // String[] fileNames = line.getOptionValues("f");
        // client.setEndpointUrl(line.getOptionValue("u"));

    }

}
