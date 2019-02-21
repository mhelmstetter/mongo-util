package com.mongodb.mongoreplay;

import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.util.TimedEvent;

public class ReplayTask implements Callable<ReplayResult> {

    private TimedEvent event;
    private Monitor monitor;
    private MongoClient mongoClient;
    private Document commandDoc;
    private String dbName;
    private Command command;
    private ReadPreference readPreference;

    protected static final Logger logger = LoggerFactory.getLogger(ReplayTask.class);

    public ReplayTask(Monitor monitor, MongoClient mongoClient, Document commandDoc, Command command, String dbName, ReadPreference readPreference) {
        this.monitor = monitor;
        this.mongoClient = mongoClient;
        this.commandDoc = commandDoc;
        this.dbName = dbName;
        this.command = command;
        this.readPreference = readPreference;
    }

    @Override
    public ReplayResult call() {

        event = new TimedEvent();
        ReplayResult replayResult = null;
        try {
            Document commandResult = null;
            if (command.isRead()) {
                commandResult = mongoClient.getDatabase(dbName).runCommand(commandDoc, readPreference);
            } else {
                commandResult = mongoClient.getDatabase(dbName).runCommand(commandDoc);
            }
            long duration = event.stop();
            Number ok = (Number)commandResult.get("ok");
            //logger.debug("result: " + result);
            if (ok.equals(1.0)) {
                event.incrementCount();
                replayResult = new ReplayResult(commandDoc, dbName, command, duration, true);
            } else {
                event.incrementError(1);
                replayResult = new ReplayResult(commandDoc, dbName, command, duration, true);
            }
            
            
        } catch (Exception e) {
            e.printStackTrace();
            event.incrementError(1);
        }

        monitor.add(event);
        return replayResult;
    }
}
