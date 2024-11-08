package com.mongodb.mongosync;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.mongodb.model.Namespace;
import com.mongodb.mongosync.model.MongoSyncApiResponse;
import com.mongodb.mongosync.model.MongoSyncState;
import com.mongodb.mongosync.model.MongoSyncStatus;
import com.mongodb.util.HttpUtils;
import com.mongodb.util.MaskUtil;
import com.mongodb.util.ThreadUtils;

public class MongoSyncRunner implements MongoSyncEventListener {

	private Gson gson = new Gson();
	
	private Thread monitorThread;
	
	private static final ReentrantLock lock = new ReentrantLock();

	public final static String[] PASSWORD_KEYS = { "--password", "--destinationPassword" };

	private boolean hasBeenPaused = false;
	private boolean hasBeenCommited = false;

	private File mongosyncBinary;
	private CommandLine cmdLine;

	private String sourceUri;
	private String destinationUri;
	private File logDir;

	private int port;
	private String baseUrl;
	private int loadLevel;

	private boolean buildIndexes;
	
	private boolean complete = false;

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

	private MongoSyncStatus mongoSyncStatus;

	private MongoSyncPauseListener pauseListener;

	public MongoSyncRunner(String id, MongoSyncPauseListener pauseListener) {
		this.id = id;
		logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
		httpUtils = new HttpUtils();
		this.pauseListener = pauseListener;
	}

	public void initialize() throws ExecuteException, IOException {

		logger.debug("initialize() id: " + id);
		baseUrl = String.format("http://localhost:%s/api/v1/", port);
		
		cmdLine = new CommandLine(mongosyncBinary);

		addArg("cluster0", sourceUri);
		addArg("cluster1", destinationUri);
		// addArg("logPath", logPath.getAbsolutePath());
		addArg("port", port);
		addArg("loadLevel", loadLevel);
		addArg("id", id);

		MongoSyncLogHandler logHandler = new MongoSyncLogHandler(this, id, logDir);
		PumpStreamHandler psh = new PumpStreamHandler(logHandler);
		// PumpStreamHandler psh = new PumpStreamHandler(new ExecBasicLogHandler(id));

		executeResultHandler = new DefaultExecuteResultHandler();
		executor = new DefaultExecutor();
		watchdog = new ExecuteWatchdog(ExecuteWatchdog.INFINITE_TIMEOUT);
		// executor.setExitValue(0);
		executor.setWatchdog(watchdog);
		executor.setStreamHandler(psh);

		logger.debug("mongosync execute id: " + id + " cmdLine: " + MaskUtil.maskCommandLine(cmdLine, PASSWORD_KEYS));
		executor.execute(cmdLine, executeResultHandler);

		// Start a separate thread to monitor the process state
		monitorThread = new Thread(this::monitorProcess);
		monitorThread.setDaemon(true);
		monitorThread.start();

	}
	
	private boolean restartProcess() {
	    try {
	        logger.info("{} - Restarting process", id);
	        initialize();  // Re-run the initialization to start a fresh process
	        return true;
	    } catch (IOException e) {
	        logger.error("Failed to restart process {}: {}", id, e.getMessage());
	        return false;
	    }
	}

	private void monitorProcess() {
	    int restartAttempts = 0;
	    final int maxRestarts = 3;
	    
	    while (restartAttempts < maxRestarts) {
	        ThreadUtils.sleep(5000);  // initial delay
	        
	        while (!executeResultHandler.hasResult()) {
	            if (!watchdog.isWatching() || watchdog.killedProcess()) {
	                logger.warn("{} - process terminated unexpectedly", id);
	                mongoSyncStatus = null;
	                
	                if (restartAttempts < maxRestarts) {
	                    restartAttempts++;
	                    logger.info("{} - Restart attempt {}/{}", id, restartAttempts, maxRestarts);
	                    
	                    if (!restartProcess()) {
	                        logger.error("{} - Restart failed. Exiting monitor.", id);
	                        return;
	                    }
	                } else {
	                    logger.error("{} - Max restart attempts reached. Exiting monitor.", id);
	                    return;
	                }
	                break;
	            }

	            checkStatus();
	            
	            if (mongoSyncStatus != null && mongoSyncStatus.getProgress().isCanCommit() && !hasBeenCommited) {
	                commit();
	                hasBeenCommited = true;
	                return;
	            }

	            ThreadUtils.sleep(10000);
	        }
	        
	        try {
	            executeResultHandler.waitFor(60 * 5 * 1000);
	        } catch (InterruptedException e) {
	            Thread.currentThread().interrupt();
	        }

	        if (executeResultHandler.getException() != null) {
	            logger.error("{} - process exited with exit code {}, error: ", id, executeResultHandler.getExitValue(), executeResultHandler.getException());
	        } else {
	            logger.info("{} - process exited normally", id);
	        }
	        complete = true;
	        return;
	    }
	}



	public MongoSyncStatus getStatus() {
		return mongoSyncStatus;
	}
	
	public MongoSyncStatus checkStatus() {
		return checkStatus(5);
	}

	private MongoSyncStatus checkStatus(int tries) {
	    try {
	        String json = httpUtils.doGetAsString(baseUrl + "progress", tries);
	        if (json != null) {
	            synchronized (this) {
	                mongoSyncStatus = gson.fromJson(json, MongoSyncStatus.class);
	            }
	            return mongoSyncStatus;
	        }
	    } catch (IOException e) {
	        logger.warn("IOException checking status: {}", e.getMessage());
	    }
	    
	    // If status checking fails multiple times, assume the process is down
	    if (mongoSyncStatus == null) {
	        logger.error("Unable to retrieve status for process {} - assuming it has terminated.", id);
	    }

	    return mongoSyncStatus;
	}

	
	private void waitForIdleStatus() {
		int sleep = 1000;
		for (int i = 1; i <= 100; i++) {
			ThreadUtils.sleep(i * sleep);
			logger.debug("mongosync {}: preflight check #{}: {}", id, i, mongoSyncStatus);
			if (mongoSyncStatus != null && mongoSyncStatus.getProgress().getState().equals(MongoSyncState.IDLE)) {
				break;
			}
		}
	}

	public void start() {
		waitForIdleStatus();
		JsonObject jsonObject = new JsonObject();
		jsonObject.addProperty("source", "cluster0");
		jsonObject.addProperty("destination", "cluster1");

		if (!buildIndexes) {
			jsonObject.addProperty("buildIndexes", "never");
		}

		if (includeNamespaces != null && !includeNamespaces.isEmpty()) {
			JsonArray includeNamespacesArray = new JsonArray();
			for (Namespace ns : includeNamespaces) {
				JsonObject inc = new JsonObject();
				inc.addProperty("database", ns.getDatabaseName());
				JsonArray collections = new JsonArray();
				collections.add(ns.getCollectionName());
				inc.add("collections", collections);
				includeNamespacesArray.add(inc);
			}
			jsonObject.add("includeNamespaces", includeNamespacesArray);
		}

		String jsonData = jsonObject.toString();

		String statusStr;
		try {
			statusStr = httpUtils.doPostAsString(baseUrl + "/start", jsonData);
			logger.debug("start result ({}): {}", port, statusStr);
		} catch (IOException e) {
			logger.error("Error starting", e);
		}
	}

	private MongoSyncApiResponse httpPost(String apiPath, String symbol) {
		MongoSyncApiResponse mongoSyncApiResponse = null;
		
		int sleep = 2000;
		for (int i = 1; i <= 15; i++) {
			lock.lock(); // Acquire the lock to ensure only one thread can proceed
			try {
				String result = httpUtils.doPostAsString(baseUrl + apiPath, "{ }");
				if (result != null) {
					mongoSyncApiResponse = gson.fromJson(result, MongoSyncApiResponse.class);
					logger.debug("{}: {} {} result: {}", id, symbol, apiPath, result);
					if (mongoSyncApiResponse.isSuccess()) {
						return mongoSyncApiResponse;
					}
				}
			} catch (IOException e) {
				logger.error("Error executing {}: {}", apiPath, e.getMessage());
			} finally {
				lock.unlock();
			}
			ThreadUtils.sleep(i * sleep);
		}
		return mongoSyncApiResponse;
	}

	public void pause() {
		httpPost("pause", "⏸️️");
	}

	public void resume() {
		httpPost("resume", "▶️");
	}

	public void commit() {
		MongoSyncApiResponse mongoSyncApiResponse = httpPost("commit", "⏹️");
		if (mongoSyncApiResponse != null && mongoSyncApiResponse.isSuccess()) {
			hasBeenCommited = true;
			watchdog.destroyProcess();
		}
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

	public void setLogDir(File logDir) {
		this.logDir = logDir;
	}

	public void setBuildIndexes(boolean buildIndexes) {
		this.buildIndexes = buildIndexes;
	}

	public void setIncludeNamespaces(List<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}
	
	private boolean isCoordinator() {
		if (mongoSyncStatus != null) {
			return mongoSyncStatus.getProgress().getCoordinatorID().equals(id);
		} else {
			logger.warn("checking isCoordinator(), mongoSyncStatus was null");
			return false;
		}
	}

	@Override
	public void readyForPauseAfterCollectionCreation() {
		if (!hasBeenPaused) {
			logger.debug("{}: ********** READY FOR PAUSE! **********", id);
			this.pause();
			if (isCoordinator()) {
				pauseListener.mongoSyncPaused();
			}
			hasBeenPaused = true;
		}

	}

	public boolean isComplete() {
		return complete;
	}
}
