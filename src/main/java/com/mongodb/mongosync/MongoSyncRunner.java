package com.mongodb.mongosync;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.model.Namespace;
import com.mongodb.mongomirror.model.InitialSyncDetails;
import com.mongodb.mongomirror.model.MongoMirrorStatus;
import com.mongodb.mongomirror.model.MongoMirrorStatusInitialSync;
import com.mongodb.mongomirror.model.MongoMirrorStatusOplogSync;
import com.mongodb.util.HttpUtils;
import com.mongodb.util.MaskUtil;

public class MongoSyncRunner {
	
	private static final int WAITFOR_TIMEOUT = 60000;

    public final static String[] PASSWORD_KEYS = {"--password", "--destinationPassword"};

    private File mongosyncBinary;
    private CommandLine cmdLine;

    private String sourceUri;
    private String destinationUri;
    private File logDir;
    
    private int port;
    private int loadLevel;
    
    private boolean buildIndexes;
    
    private List<Namespace> includeNamespaces;
    
   
    private int errorCount;

    private String id;

    private Logger logger;

    private HttpUtils httpUtils;
//    private MongoMirrorLogHandler logHandler;
//    private EmailSender emailSender;
    
    private DefaultExecutor executor;
    private ExecuteWatchdog watchdog;
    private DefaultExecuteResultHandler executeResultHandler;

    public MongoSyncRunner(String id) {
        this.id = id;
        logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
        httpUtils = new HttpUtils();
    }

    public void initialize() throws ExecuteException, IOException {

        logger.debug("initialize() id: " + id);
        
		cmdLine = new CommandLine(mongosyncBinary);
		
		File logPath = new File(logDir, "mongosync_" + id);

        addArg("cluster0", sourceUri);
        addArg("cluster1", destinationUri);
        addArg("logPath", logPath.getAbsolutePath());
        addArg("port", port);
        addArg("loadLevel", loadLevel);
        addArg("id", id);
        
        PumpStreamHandler psh = new PumpStreamHandler(new ExecBasicLogHandler(id));

        executeResultHandler = new DefaultExecuteResultHandler();
        executor = new DefaultExecutor();
        watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
        //executor.setExitValue(0);
        executor.setWatchdog(watchdog);
        executor.setStreamHandler(psh);

        logger.debug("mongosync execute id: " + id + " cmdLine: " + MaskUtil.maskCommandLine(cmdLine, PASSWORD_KEYS));
        executor.execute(cmdLine, executeResultHandler);

    }
    
    public void start() {
    	JsonObject jsonObject = new JsonObject();
    	jsonObject.addProperty("source", "cluster0");
    	jsonObject.addProperty("destination", "cluster1");
    	
    	if (! buildIndexes) {
    		jsonObject.addProperty("buildIndexes", "cluster1");
    	}
    	
    	if (includeNamespaces != null && ! includeNamespaces.isEmpty()) {
    		JsonArray includeNamespacesArray = new JsonArray();
    		for (Namespace ns : includeNamespaces) {
        		JsonObject inc = new JsonObject();
        		inc.addProperty("database", ns.getDatabaseName());
        		JsonArray collections = new JsonArray();
        		collections.add(ns.getCollectionName());
        		inc.add("collections", collections);
        		includeNamespacesArray.add(inc);
        	}
    	}
    	
    	
    	String jsonData = jsonObject.toString();
    	
    	logger.debug("***** json config: {}", jsonData);
    	
    }

    private void processError(String error) {
    	errorCount++;
    	//if (errorCount >= 5 && bookmarkFileExists()) {
    		
    		ExecuteWatchdog wd = executor.getWatchdog();
    		logger.warn("stopping process after {} errors", errorCount);
    		//logHandler.getListener().procLoggedError("***** stopping mongomirror");
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
    		//logHandler.getListener().procLoggedError("***** mongomirror restarted");
    		errorCount = 0;
    	//}
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

    public void setExecuteResultHandler(DefaultExecuteResultHandler executeResultHandler) {
        this.executeResultHandler = executeResultHandler;
    }

    public void setSourceUri(String sourceHost) {
        this.sourceUri = sourceHost;
    }

    public void setDestinationUri(String destinationHost) {
        this.destinationUri = destinationHost;
    }

    public String getId() {
        return id;
    }

    public int getErrorCount() {
		return errorCount;
	}

	public File getMongosyncBinary() {
		return mongosyncBinary;
	}

	public void setMongosyncBinary(File mongosyncBinary) {
		this.mongosyncBinary = mongosyncBinary;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setLoadLevel(int loadLevel) {
		this.loadLevel = loadLevel;
	}
	
    class ExecBasicLogHandler extends LogOutputStream {
        
        private PrintWriter writer;
        
        public ExecBasicLogHandler(String id) throws IOException {
            super();
            writer = new PrintWriter(new FileWriter(new File(id + ".log")));
        }


        protected void processLine(String line) {
            writer.println(line);
            writer.flush();
        }


        @Override
        protected void processLine(String line, int logLevel) {
            writer.println(line);
            writer.flush();
        }
    }

	public void setLogDir(File logDir) {
		this.logDir = logDir;
	}

	public void setBuildIndexes(boolean buildIndexes) {
		this.buildIndexes = buildIndexes;
	}

	public void setIncludeNamespaces(List<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}
}
