package com.mongodb.mongomirror;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.model.Namespace;
import com.mongodb.mongomirror.model.InitialSyncDetails;
import com.mongodb.mongomirror.model.MongoMirrorStatus;
import com.mongodb.mongomirror.model.MongoMirrorStatusInitialSync;
import com.mongodb.mongomirror.model.MongoMirrorStatusOplogSync;
import com.mongodb.util.HttpUtils;
import com.mongodb.util.MaskUtil;

public class MongoMirrorRunner {
	
	private static final int WAITFOR_TIMEOUT = 60000;

    public final static String[] PASSWORD_KEYS = {"--password", "--destinationPassword"};

    private File mongomirrorBinary;
    private CommandLine cmdLine;

    private String sourceHost;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceAuthenticationDatabase;
    private String sourceAuthenticationMechanism;
    private Boolean sourceSsl;

    private String destinationHost;
    private String destinationUsername;
    private String destinationPassword;
    private String destinationAuthenticationDatabase;
    private String destinationAuthenticationMechanism;

    private String readPreference;
    private Boolean drop;
    private String bookmarkFile;
    private String compressors;
    private Integer httpStatusPort;
    private String oplogPath;
    private Integer collStatsThreshold;

    private String numParallelCollections;

    // Custom mongomirror options
    private String writeConcern;
    private Boolean destinationNoSSL;
    private Boolean noIndexRestore;
    private Boolean extendTtl;
    private Boolean noCollectionCreate = true;
    private Integer stopWhenLagWithin;

    private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
    private Set<String> includeDatabases = new HashSet<String>();
    private List<String> emailRecipients = new ArrayList<>();

    private int errMsgWindowSecs;
    private int errorRptMaxErrors;
    private int totalEmailsMax;
    private String smtpHost;
    private int smtpPort;
    private boolean smtpTls;
    private boolean smtpAuth;
    private String smtpPassword;
    private String emailFrom;
    
    private int errorCount;

    private String id;

    private Logger logger;

    private HttpUtils httpUtils;
    private MongoMirrorLogHandler logHandler;
    private EmailSender emailSender;
    
    private DefaultExecutor executor;
    private ExecuteWatchdog watchdog;
    private DefaultExecuteResultHandler executeResultHandler;

    public MongoMirrorRunner(String id) {
        this.id = id;
        logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
        httpUtils = new HttpUtils();
    }

    public void execute(boolean dryRun) throws ExecuteException, IOException {

        logger.debug("execute() start id: " + id);
        
        cmdLine = new CommandLine(mongomirrorBinary);

        addArg("host", sourceHost);
        addArg("username", sourceUsername);
        addArg("password", sourcePassword);
        addArg("authenticationDatabase", sourceAuthenticationDatabase);
        addArg("authenticationMechanism", sourceAuthenticationMechanism);
        addArg("ssl", sourceSsl);

        addArg("destination", destinationHost);
        addArg("destinationUsername", destinationUsername);
        addArg("destinationPassword", destinationPassword);
        addArg("destinationAuthenticationDatabase", destinationAuthenticationDatabase);
        addArg("destinationAuthenticationMechanism", destinationAuthenticationMechanism);

        addArg("readPreference", readPreference);
        addArg("destinationNoSSL", destinationNoSSL);
        addArg("extendTTL", extendTtl);
        addArg("noCollectionCreate", noCollectionCreate);

        addArg("drop", drop);
        addArg("bookmarkFile", bookmarkFile);
        addArg("numParallelCollections", numParallelCollections);
        addArg("compressors", compressors);
        addArg("writeConcern", writeConcern);
        addArg("httpStatusPort", httpStatusPort);
        addArg("oplogPath", oplogPath);
        addArg("noIndexRestore", noIndexRestore);
        addArg("collStatsThreshold", collStatsThreshold);
        addArg("stopWhenLagWithin", stopWhenLagWithin);
        addArg("resumeDBFile", "mongomirror_resume_" + id + ".db");

        for (Namespace ns : includeNamespaces) {
            addArg("includeNamespace", ns.getNamespace());
        }

        for (String dbName : includeDatabases) {
            addArg("includeDB", dbName);
        }

        if (dryRun) {
            logger.debug("dry run: " + id + " cmdLine: " + cmdLine);
            return;
        }
        
        
        if (!emailRecipients.isEmpty()) {
            emailSender = new EmailSender(emailRecipients, smtpHost, smtpPort, smtpTls, smtpAuth, emailFrom,
                    smtpPassword, errMsgWindowSecs, errorRptMaxErrors, totalEmailsMax, id);
        }
        logHandler = new MongoMirrorLogHandler(emailSender, id);
        PumpStreamHandler psh = new PumpStreamHandler(logHandler);

        executeResultHandler = new DefaultExecuteResultHandler();
        executor = new DefaultExecutor();
        watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        //executor.setExitValue(0);
        executor.setWatchdog(watchdog);
        executor.setStreamHandler(psh);

        logger.debug("mongomirror execute id: " + id + " cmdLine: " + MaskUtil.maskCommandLine(cmdLine, PASSWORD_KEYS));
        executor.execute(cmdLine, executeResultHandler);
        

//        try {
//            executeResultHandler.waitFor();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        logHandler.getListener().procLoggedComplete("MongoMirror execution completed");

    }

    @SuppressWarnings("rawtypes")
    public MongoMirrorStatus checkStatus() {
        String statusStr = null;

        try {
            statusStr = httpUtils.doGetAsString(String.format("http://localhost:%s", httpStatusPort));
            return parseStatus(statusStr);

        } catch (IOException e) {
            logger.error(statusStr);
            logger.error("Error checking mongomirror status: " + e.getMessage());
            processError(e.getMessage());
        } catch (Exception e) {
            logger.error(statusStr);
            logger.error("Error checking mongomirror status", e);
            processError(e.getMessage());
        }
        return null;
    }
    
    private boolean bookmarkFileExists() {
    	Path path = Paths.get(bookmarkFile);
    	return Files.exists(path);
    }
    
    private void processError(String error) {
    	errorCount++;
    	if (errorCount >= 5 && bookmarkFileExists()) {
    		
    		ExecuteWatchdog wd = executor.getWatchdog();
    		logger.warn("stopping process after {} errors", errorCount);
    		logHandler.getListener().procLoggedError("***** stopping mongomirror");
    		wd.destroyProcess();
    		try {
				executeResultHandler.waitFor(WAITFOR_TIMEOUT);
			} catch (InterruptedException e1) {
				logger.warn("executeResultHandler interrupted", e1);
			}
    		try {
				executor.execute(cmdLine, executeResultHandler);
			} catch (IOException e) {
				logger.error("error restarting process", e);
			}
    		logHandler.getListener().procLoggedError("***** mongomirror restarted");
    		errorCount = 0;
    	}
    }

    public MongoMirrorStatus parseStatus(String statusStr) {
        Gson gson = new GsonBuilder().create();

        JsonObject statusJson = new JsonParser().parse(statusStr).getAsJsonObject();

        String stage = null;
        String phase = null;
        String errorMessage = null;

        if (statusJson.has("stage")) {
            stage = statusJson.get("stage").getAsString();
        }

        if (statusJson.has("phase")) {
            phase = statusJson.get("phase").getAsString();
        }

        if (statusJson.has("errorMessage")) {
            errorMessage = statusJson.get("errorMessage").getAsString();
            processError(errorMessage);
        }

        if ("initial sync".equals(stage) && !"applying oplog entries".equals(phase)) {

            MongoMirrorStatusInitialSync status = new MongoMirrorStatusInitialSync(stage, phase, errorMessage);

            if (statusJson.has("details")) {

                JsonObject statusDetails = statusJson.get("details").getAsJsonObject();

                if (statusDetails.has("copiedBytesAllColl")) {
                    InitialSyncDetails details = new InitialSyncDetails();
                    status.setTopLevelDetails(details);
                    details.setCopiedBytes(statusDetails.get("copiedBytesAllColl").getAsLong());
                    details.setTotalBytes(statusDetails.get("totalBytesAllColl").getAsLong());

                } else {
                    status = gson.fromJson(statusJson, MongoMirrorStatusInitialSync.class);
                }
            } else {
                logger.warn("InitialSyncDetails was missing from http status output");
            }
            return status;
        } else if ("applying oplog entries".equals(phase) || "oplog sync".equals(phase)) {
            //MongoMirrorStatusOplogSync status = new MongoMirrorStatusOplogSync(stage, phase, errorMessage);
            MongoMirrorStatusOplogSync status = gson.fromJson(statusJson, MongoMirrorStatusOplogSync.class);
            return status;
        } else {
            MongoMirrorStatus status = new MongoMirrorStatus(stage, phase, errorMessage);
            return status;
        }
    }

    private void addArg(String argName) {
        cmdLine.addArgument("--" + argName);
    }

    private void addArg(String argName, Integer argValue) {
        if (argValue != null) {
            cmdLine.addArgument("--" + argName + "=" + argValue);
        }
    }

    private void addArg(String argName, String argValue) {
        if (argValue != null) {
            cmdLine.addArgument("--" + argName + "=" + argValue);
        }
    }

    private void addArg(String argName, Boolean argValue) {
        if (argValue != null && argValue) {
            cmdLine.addArgument("--" + argName);
        }
    }

    public void setMongomirrorBinary(File mongomirrorBinary) {
        this.mongomirrorBinary = mongomirrorBinary;
    }

    public File getMongomirrorBinary() {
        return mongomirrorBinary;
    }

    public void setExecuteResultHandler(DefaultExecuteResultHandler executeResultHandler) {
        this.executeResultHandler = executeResultHandler;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public void setSourceAuthenticationDatabase(String sourceAuthenticationDatabase) {
        this.sourceAuthenticationDatabase = sourceAuthenticationDatabase;
    }

    public void setSourceAuthenticationMechanism(String sourceAuthenticationMechanism) {
        this.sourceAuthenticationMechanism = sourceAuthenticationMechanism;
    }

    public void setDestinationHost(String destinationHost) {
        this.destinationHost = destinationHost;
    }

    public void setDestinationUsername(String destinationUsername) {
        this.destinationUsername = destinationUsername;
    }

    public void setDestinationPassword(String destinationPassword) {
        this.destinationPassword = destinationPassword;
    }

    public void setDestinationAuthenticationDatabase(String destinationAuthenticationDatabase) {
        this.destinationAuthenticationDatabase = destinationAuthenticationDatabase;
    }

    public void setDestinationAuthenticationMechanism(String destinationAuthenticationMechanism) {
        this.destinationAuthenticationMechanism = destinationAuthenticationMechanism;
    }

    public void setReadPreference(String readPreference) {
        this.readPreference = readPreference;
    }

    public void setDrop(Boolean drop) {
        this.drop = drop;
    }

    public void setDestinationNoSSL(Boolean destinationNoSSL) {
        this.destinationNoSSL = destinationNoSSL;
    }

    public void setBookmarkFile(String bookmarkFile) {
        this.bookmarkFile = bookmarkFile;
    }

    public void setSourceSsl(Boolean sourceSsl) {
        this.sourceSsl = sourceSsl;
    }

    public void setNumParallelCollections(String numParallelCollections) {
        this.numParallelCollections = numParallelCollections;
    }

    public void addIncludeNamespace(Namespace ns) {
        includeNamespaces.add(ns);
    }

    public void addIncludeDatabase(String dbName) {
        includeDatabases.add(dbName);
    }

    public void setCompressors(String compressors) {
        this.compressors = compressors;
    }

    public void setWriteConcern(String writeConcern) {
        this.writeConcern = writeConcern;
    }

    public void setHttpStatusPort(Integer httpStatusPort) {
        this.httpStatusPort = httpStatusPort;
    }

    public String getId() {
        return id;
    }

    public void setOplogPath(String oplogPath) {
        this.oplogPath = oplogPath;
    }

    public void setNoIndexRestore(Boolean noIndexRestore) {
        this.noIndexRestore = noIndexRestore;
    }

    public void setCollStatsThreshold(Integer collStatsThreshold) {
        this.collStatsThreshold = collStatsThreshold;
    }

    public void setExtendTtl(Boolean extendTtl) {
        this.extendTtl = extendTtl;
    }

    public void addEmailRecipient(String address) {
        emailRecipients.add(address);
    }

    public void setStopWhenLagWithin(int stopWhenLagWithin) {
        this.stopWhenLagWithin = stopWhenLagWithin;
    }

    public int getErrMsgWindowSecs() {
        return errMsgWindowSecs;
    }

    public void setErrMsgWindowSecs(int errMsgWindowSecs) {
        this.errMsgWindowSecs = errMsgWindowSecs;
    }

    public int getErrorRptMaxErrors() {
        return errorRptMaxErrors;
    }

    public void setErrorRptMaxErrors(int errorRptMaxErrors) {
        this.errorRptMaxErrors = errorRptMaxErrors;
    }

    public int getTotalEmailsMax() {
        return totalEmailsMax;
    }

    public void setTotalEmailsMax(int totalEmailsMax) {
        this.totalEmailsMax = totalEmailsMax;
    }

    public String getSmtpHost() {
        return smtpHost;
    }

    public void setSmtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    public int getSmtpPort() {
        return smtpPort;
    }

    public void setSmtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    public boolean isSmtpTls() {
        return smtpTls;
    }

    public void setSmtpTls(boolean smtpTls) {
        this.smtpTls = smtpTls;
    }

    public boolean isSmtpAuth() {
        return smtpAuth;
    }

    public void setSmtpAuth(boolean smtpAuth) {
        this.smtpAuth = smtpAuth;
    }

    public String getSmtpPassword() {
        return smtpPassword;
    }

    public void setSmtpPassword(String smtpPassword) {
        this.smtpPassword = smtpPassword;
    }

    public String getEmailFrom() {
        return emailFrom;
    }

    public void setEmailFrom(String emailFrom) {
        this.emailFrom = emailFrom;
    }

	public void setNoCollectionCreate(Boolean noCollectionCreate) {
		this.noCollectionCreate = noCollectionCreate;
	}

	public int getErrorCount() {
		return errorCount;
	}
}
