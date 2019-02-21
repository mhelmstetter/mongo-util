package com.mongodb.mongoreplay;

import org.bson.Document;

public class ReplayResult {
    
    private Document commandDoc;
    private String dbName;
    private String collName;
    private Command command;
    private boolean success;
    private double duration;
    
    
    public ReplayResult(Document commandDoc, String dbName, Command command, long duration, boolean success) {
        this.commandDoc = commandDoc;
        this.dbName = dbName;
        this.command = command;
        this.success = success;
        this.duration = duration;
    }


    public Document getCommandDoc() {
        return commandDoc;
    }


    public String getDbName() {
        return dbName;
    }


    public Command getCommand() {
        return command;
    }


    public boolean isSuccess() {
        return success;
    }


    public String getCollName() {
        return collName;
    }


    public void setCollName(String collName) {
        this.collName = collName;
    }
    
    public String getNamespace() {
        return dbName + "." + collName;
    }


    public double getDuration() {
        return duration;
    }

}
