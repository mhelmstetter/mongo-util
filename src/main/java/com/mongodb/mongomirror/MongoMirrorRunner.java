package com.mongodb.mongomirror;

import java.io.File;
import java.io.IOException;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecuteResultHandler;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteResultHandler;

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
    private Boolean destinationNoSSL;
    private String namespaceFilter;
    private String bookmarkFile;

   
    public void execute() throws ExecuteException, IOException {
        
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
        addArg("drop", drop);
        addArg("filter", namespaceFilter);
        addArg("bookmarkFile", bookmarkFile);
        
        
        DefaultExecutor executor = new DefaultExecutor();
        executor.setExitValue(1);
        executor.execute(cmdLine, executeResultHandler);
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
    
    public static void main(String[] args) throws Exception {
        MongoMirrorRunner mongoMirror = new MongoMirrorRunner();
        mongoMirror.setMongomirrorBinary(new File("/Users/mh/go/src/github.com/10gen/mongomirror/build/mongomirror"));
        mongoMirror.execute();

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

    public void setNamespaceFilter(String namespaceFilter) {
        this.namespaceFilter = namespaceFilter;
    }

    public void setBookmarkFile(String bookmarkFile) {
        this.bookmarkFile = bookmarkFile;
    }

    public void setSourceSsl(Boolean sourceSsl) {
        this.sourceSsl = sourceSsl;
    }

}
