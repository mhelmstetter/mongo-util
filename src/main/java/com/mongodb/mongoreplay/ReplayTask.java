package com.mongodb.mongoreplay;

import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.util.Monitor;
import com.mongodb.util.TimedEvent;

public class ReplayTask implements Callable<Object> {

    private TimedEvent event;
    private Monitor monitor;
    private MongoClient mongoClient;
    private Document commandDoc;
    private String dbName;
    private CommandType commandType;

    protected static final Logger logger = LoggerFactory.getLogger(ReplayTask.class);

    public ReplayTask(Monitor monitor, MongoClient mongoClient, Document commandDoc, CommandType commandType, String dbName) {
        this.monitor = monitor;
        this.mongoClient = mongoClient;
        this.commandDoc = commandDoc;
        this.dbName = dbName;
        this.commandType = commandType;
    }

    @Override
    public Object call() {

        event = new TimedEvent();
        try {
            Document result = null;
            if (commandType.equals(CommandType.READ)) {
                result = mongoClient.getDatabase(dbName).runCommand(commandDoc, ReadPreference.secondary());
            } else {
                result = mongoClient.getDatabase(dbName).runCommand(commandDoc);
            }
            
            Number ok = (Number)result.get("ok");
            //logger.debug("result: " + result);
            if (ok.equals(1.0)) {
                event.incrementCount();
            } else {
                event.incrementError(1);
            }
            
            
        } catch (Exception e) {
            e.printStackTrace();
            event.incrementError(1);
        }

        monitor.add(event);
        return null;
    }
}
