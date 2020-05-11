package com.mongodb.mongomirror;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.model.Namespace;
import com.mongodb.util.HttpUtils;

public class MongoMirrorRunner {
    
    private File mongomirrorBinary;
    private CommandLine cmdLine;
    
    private ExecuteResultHandler executeResultHandler;
    
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
    private Boolean preserveUUIDs;
    private String bookmarkFile;
    private String compressors;
    private Integer httpStatusPort;
    private String oplogPath;
    private Integer collStatsThreshold;
    
    private String numParallelCollections;
    
    // Custom mongomirror options
    private String writeConcern;
    private Boolean destinationNoSSL;
    private Boolean skipBuildIndexes;
    
    private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
    private Set<String> includeDatabases = new HashSet<String>();

    private String id;
    
    private Logger logger;
    
    private HttpUtils httpUtils;
    private Gson gson;
    
    public MongoMirrorRunner(String id) {
        this.id = id;
        logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
        httpUtils = new HttpUtils();
        gson = new GsonBuilder().create();
    }
   
    public void execute() throws ExecuteException, IOException {
        
        logger.debug("execute() start id: " + id);
        executeResultHandler = new DefaultExecuteResultHandler();
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
        addArg("preserveUUIDs", preserveUUIDs);
        
        addArg("drop", drop);
        addArg("bookmarkFile", bookmarkFile);
        addArg("numParallelCollections", numParallelCollections);
        addArg("compressors", compressors);
        addArg("writeConcern", writeConcern);
        addArg("httpStatusPort", httpStatusPort);
        addArg("oplogPath", oplogPath);
        addArg("skipBuildIndexes", skipBuildIndexes);
        addArg("collStatsThreshold", collStatsThreshold);
        
        for (Namespace ns : includeNamespaces) {
            addArg("includeNamespace", ns.getNamespace());
        }
        
        for (String dbName : includeDatabases) {
            addArg("includeDB", dbName);
        }
        
        PumpStreamHandler psh = new PumpStreamHandler(new ExecBasicLogHandler(id));
        
        DefaultExecutor executor = new DefaultExecutor();
        executor.setExitValue(1);
        executor.setStreamHandler(psh);
        logger.debug("executor.execute id: " + id + " cmdLine: " + cmdLine);
        executor.execute(cmdLine, executeResultHandler);
    }
    
    @SuppressWarnings("rawtypes")
    public MongoMirrorStatus checkStatus() {
        String statusStr = null;
        MongoMirrorStatus status = null;
        try {
            statusStr = httpUtils.doGetAsString(String.format("http://localhost:%s", httpStatusPort));
            if (statusStr.contains("\"stage\":\"initial sync\"")) {
                if (statusStr.contains("\"applying oplog entries\"")) {
                    status = gson.fromJson(statusStr, MongoMirrorStatusOplogSync.class);
                } else {
                    status = gson.fromJson(statusStr, MongoMirrorStatusInitialSync.class);
                }
                
            } else if (statusStr.contains("\"stage\":\"oplog sync\"")) {
                status = gson.fromJson(statusStr, MongoMirrorStatusOplogSync.class);
            } else {
                status = gson.fromJson(statusStr, MongoMirrorStatus.class);
            }
        } catch (IOException e) {
            logger.error(statusStr);
            logger.error("Error checking mongomirror status: " + e.getMessage());
        } catch (Exception e) {
            logger.error(statusStr);
            logger.error("Error checking mongomirror status", e);
        }
        return status;
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

    public void setExecuteResultHandler(ExecuteResultHandler executeResultHandler) {
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
    
    class ExecLogHandler extends LogOutputStream {
        private Logger log;

        public ExecLogHandler(Logger log) {
            super();
            this.log = log;
        }


        protected void processLine(String line) {
            log.debug(line);
        }


        @Override
        protected void processLine(String line, int logLevel) {
            log.debug(line);
        }
    }

    public void setNumParallelCollections(String numParallelCollections) {
        this.numParallelCollections = numParallelCollections;
    }

    public Boolean getPreserveUUIDs() {
        return preserveUUIDs;
    }

    public void setPreserveUUIDs(Boolean preserveUUIDs) {
        this.preserveUUIDs = preserveUUIDs;
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

	public void setSkipBuildIndexes(Boolean skipBuildIndexes) {
		this.skipBuildIndexes = skipBuildIndexes;
	}

	public void setCollStatsThreshold(Integer collStatsThreshold) {
		this.collStatsThreshold = collStatsThreshold;
	}

}
