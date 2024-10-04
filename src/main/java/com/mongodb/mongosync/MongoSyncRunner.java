package com.mongodb.mongosync;

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
import com.mongodb.util.HttpUtils;
import com.mongodb.util.MaskUtil;

public class MongoSyncRunner {
	
	private static final int WAITFOR_TIMEOUT = 60000;

    public final static String[] PASSWORD_KEYS = {"--password", "--destinationPassword"};

    private File mongosyncBinary;
    private CommandLine cmdLine;

    

    private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
    private Set<String> includeDatabases = new HashSet<String>();

    private String id;

    private Logger logger;

    private HttpUtils httpUtils;
    
    private DefaultExecutor executor;
    private ExecuteWatchdog watchdog;
    private DefaultExecuteResultHandler executeResultHandler;

    public MongoSyncRunner(String id) {
        this.id = id;
        logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
        httpUtils = new HttpUtils();
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

    public void setMongosyncBinary(File mongomirrorBinary) {
        this.mongosyncBinary = mongomirrorBinary;
    }

    public File getMongosyncBinary() {
        return mongosyncBinary;
    }

    public void setExecuteResultHandler(DefaultExecuteResultHandler executeResultHandler) {
        this.executeResultHandler = executeResultHandler;
    }

    public void addIncludeNamespace(Namespace ns) {
        includeNamespaces.add(ns);
    }

    public void addIncludeDatabase(String dbName) {
        includeDatabases.add(dbName);
    }

    public String getId() {
        return id;
    }
}
