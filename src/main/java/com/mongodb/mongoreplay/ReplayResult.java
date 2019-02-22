package com.mongodb.mongoreplay;

import org.bson.Document;

public class ReplayResult {
    
    private String dbName;
    private String collectionName;
    private Command command;
    private boolean success;
    private double duration;
    private String queryShape;
    
    
    public ReplayResult(String queryShape, String dbName, String collectionName, Command command, long duration, boolean success) {
        this.queryShape = queryShape;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.command = command;
        this.success = success;
        this.duration = duration;
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

    public double getDuration() {
        return duration;
    }


    public String getQueryShape() {
        return queryShape;
    }


    public String getCollectionName() {
        return collectionName;
    }

}
