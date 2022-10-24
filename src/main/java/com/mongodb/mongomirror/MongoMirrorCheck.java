package com.mongodb.mongomirror;

import com.mongodb.util.MaskUtil;
import org.apache.commons.exec.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.mongodb.mongomirror.MongoMirrorRunner.PASSWORD_KEYS;

public class MongoMirrorRunner2 implements MMEventListener {
    private final String id;
    private final Logger logger;
    private ExecuteResultHandler mmResultHandler;
    private String mongomirrorBinary;
    private CommandLine cmdLine;
    private String sourceHost;
    private String destinationHost;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceAuthenticationDatabase;
    private String sourceAuthenticationMechanism;
    private Boolean sourceSsl;
    private String destinationUsername;
    private String destinationPassword;
    private String destinationAuthenticationDatabase;
    private boolean destinationNoSSL;

    public MongoMirrorRunner2(String id) {
        this.id = id;
        logger = LoggerFactory.getLogger(this.getClass().getName() + "." + id);
    }

    public void execute(boolean dryRun) throws ExecuteException, IOException {
        logger.debug("execute() start id: " + id);
        mmResultHandler = new DefaultExecuteResultHandler();
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
        addArg("destinationNoSSL", destinationNoSSL);
        PumpStreamHandler psh = new PumpStreamHandler(new MMLogHandler(this));

        DefaultExecutor executor = new DefaultExecutor();
        executor.setExitValue(0);
        executor.setStreamHandler(psh);

        logger.debug("mongomirror execute id: " + id + " cmdLine: " + MaskUtil.maskCommandLine(cmdLine, PASSWORD_KEYS));
        executor.execute(cmdLine, mmResultHandler);
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

    public void setMongomirrorBinary(String mongomirrorBinary) {
        this.mongomirrorBinary = mongomirrorBinary;
    }

    public String getMongomirrorBinary() {
        return mongomirrorBinary;
    }

    public String getSourceHost() {
        return sourceHost;
    }

    public void setSourceHost(String sourceHost) {
        this.sourceHost = sourceHost;
    }

    public String getDestinationHost() {
        return destinationHost;
    }

    public void setDestinationHost(String destinationHost) {
        this.destinationHost = destinationHost;
    }

    public String getSourceUsername() {
        return sourceUsername;
    }

    public void setSourceUsername(String sourceUsername) {
        this.sourceUsername = sourceUsername;
    }

    public String getSourcePassword() {
        return sourcePassword;
    }

    public void setSourcePassword(String sourcePassword) {
        this.sourcePassword = sourcePassword;
    }

    public String getDestinationUsername() {
        return destinationUsername;
    }

    public void setDestinationUsername(String destinationUsername) {
        this.destinationUsername = destinationUsername;
    }

    public String getDestinationPassword() {
        return destinationPassword;
    }

    public void setDestinationPassword(String destinationPassword) {
        this.destinationPassword = destinationPassword;
    }

    public String getSourceAuthenticationDatabase() {
        return sourceAuthenticationDatabase;
    }

    public void setSourceAuthenticationDatabase(String sourceAuthenticationDatabase) {
        this.sourceAuthenticationDatabase = sourceAuthenticationDatabase;
    }

    public String getSourceAuthenticationMechanism() {
        return sourceAuthenticationMechanism;
    }

    public void setSourceAuthenticationMechanism(String sourceAuthenticationMechanism) {
        this.sourceAuthenticationMechanism = sourceAuthenticationMechanism;
    }

    public Boolean getSourceSsl() {
        return sourceSsl;
    }

    public void setSourceSsl(Boolean sourceSsl) {
        this.sourceSsl = sourceSsl;
    }

    public String getDestinationAuthenticationDatabase() {
        return destinationAuthenticationDatabase;
    }

    public void setDestinationAuthenticationDatabase(String destinationAuthenticationDatabase) {
        this.destinationAuthenticationDatabase = destinationAuthenticationDatabase;
    }

    public boolean isDestinationNoSSL() {
        return destinationNoSSL;
    }

    public void setDestinationNoSSL(boolean destinationNoSSL) {
        this.destinationNoSSL = destinationNoSSL;
    }

    @Override
    public void procFailed(Exception e) {
        System.out.println("PROC FAILED!!!!!!");
        e.printStackTrace();
    }

    @Override
    public void procLoggedError(String msg) {
        System.out.println("SAW AN ERROR!!!!!!");
        System.out.println("It was " + msg + "!!!!!!!");
    }

    @Override
    public void procLoggedComplete(String msg) {
        System.out.println("ALL DONE!!!!!!!");
        System.exit(0);
    }

    public static void main(String[] args) {

        String SRC_HOST = "atlas-agi1p1-shard-0/ac-x96bfmt-shard-00-00.lgefwkw.mongodb.net:27017,ac-x96bfmt-shard-00-01.lgefwkw.mongodb.net:27017,ac-x96bfmt-shard-00-02.lgefwkw.mongodb.net:27017";
        String DST_HOST = "atlas-k5kxfx-shard-0/cluster1-shard-00-00.xmcdw.mongodb.net:27017,cluster1-shard-00-01.xmcdw.mongodb.net:27017,cluster1-shard-00-02.xmcdw.mongodb.net:27017";
        MongoMirrorRunner mmr = new MongoMirrorRunner("123");
        mmr.setSourceHost(SRC_HOST);
        mmr.setDestinationHost(DST_HOST);
        mmr.setMongomirrorBinary(new File(String.format("%s/build/mongomirror", System.getProperty("user.dir"))));
        mmr.setSourceUsername("matt");
        mmr.setSourcePassword("tc1234");
        mmr.setSourceAuthenticationDatabase("admin");
        mmr.setSourceSsl(true);
        mmr.setDestinationUsername("matt");
        mmr.setDestinationPassword("tc1234");
        mmr.setDestinationAuthenticationDatabase("admin");
        mmr.setDestinationNoSSL(false);
        try {
            mmr.execute(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
