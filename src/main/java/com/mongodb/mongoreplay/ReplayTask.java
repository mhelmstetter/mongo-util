package com.mongodb.mongoreplay;

import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.ReadPreference;
import com.mongodb.util.TimedEvent;

public class ReplayTask implements Callable<ReplayResult> {

    // private TimedEvent event;
    private Monitor monitor;
    private MongoClient mongoClient;
    private Document commandDoc;
    private String dbName;
    private String collectionName;
    private Command command;
    private ReadPreference readPreference;
    private String queryShape;

    protected static final Logger logger = LoggerFactory.getLogger(ReplayTask.class);

    public ReplayTask(Monitor monitor, MongoClient mongoClient, Document commandDoc, Command command, String dbName,
            String collectionName, ReadPreference readPreference, String queryShape) {
        this.monitor = monitor;
        this.mongoClient = mongoClient;
        this.commandDoc = commandDoc;
        this.dbName = dbName;
        this.collectionName = collectionName;
        this.command = command;
        this.readPreference = readPreference;
        this.queryShape = queryShape;
    }

    @Override
    public ReplayResult call() {

        // event = new TimedEvent();
        long start = System.nanoTime();
        ReplayResult replayResult = null;
        try {
            Document commandResult = null;
            if (command.isRead()) {

                 commandResult = mongoClient.getDatabase(dbName).runCommand(commandDoc, readPreference);
            } else {
                commandResult = mongoClient.getDatabase(dbName).runCommand(commandDoc);
            }
            long duration = System.nanoTime() - start;
            // long duration = event.stop();
            Number ok = (Number) commandResult.get("ok");
            // logger.debug("result: " + result);
            if (ok.equals(1.0)) {
                monitor.incrementEventCount();
                replayResult = new ReplayResult(queryShape, dbName, collectionName, command, duration, true);
            } else {
                // event.incrementError(1);
                replayResult = new ReplayResult(queryShape, dbName, collectionName, command, duration, true);
                monitor.incrementErrorCount();
            }

        } catch (Exception e) {
            logger.error("Error executing task", e);
            monitor.incrementErrorCount();
        }

        // monitor.add(event);
        return replayResult;
    }
}
