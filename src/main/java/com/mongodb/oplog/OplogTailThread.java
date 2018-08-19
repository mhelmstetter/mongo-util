package com.mongodb.oplog;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.Bytes;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoCursorNotFoundException;

public class OplogTailThread {

    protected static final Logger logger = LoggerFactory.getLogger(OplogTailThread.class);
    
    private DBCollection oplog;
    private Object lastTimestamp;
    protected long reportInterval = 1000;
    private boolean killMe;

    
    
    ThreadPoolExecutor pool;

    public OplogTailThread() {
        
    }

    public void run() {

        long lastWrite = 0;
        long startTime = System.currentTimeMillis();
        long lastOutput = System.currentTimeMillis();

        try {

            while (true) {
                try {
                    DBCursor cursor = oplog.find();
                    cursor.addOption(Bytes.QUERYOPTION_TAILABLE);
                    cursor.addOption(Bytes.QUERYOPTION_AWAITDATA);
                    long count = 0;
                    long skips = 0;

                    while (!killMe && cursor.hasNext()) {
                        DBObject x = cursor.next();
                        if (!killMe) {
                            lastTimestamp = (BSONTimestamp) x.get("ts");
                            count++;
                            if (System.currentTimeMillis() - lastWrite > 1000) {
                                // writeLastTimestamp(lastTimestamp);
                                lastWrite = System.currentTimeMillis();
                            }
                            long duration = System.currentTimeMillis() - lastOutput;
                            if (duration > reportInterval) {
                                report("Oplog tail", count, skips, System.currentTimeMillis() - startTime);
                                lastOutput = System.currentTimeMillis();
                            }
                        }
                    }
                } catch (MongoCursorNotFoundException ex) {
                    // writeLastTimestamp(lastTimestamp);
                    System.out.println("Cursor not found, waiting");
                    Thread.sleep(2000);
                } catch (com.mongodb.MongoInternalException ex) {
                    System.out.println("Cursor not found, waiting");
                    // writeLastTimestamp(lastTimestamp);
                    ex.printStackTrace();
                } catch (com.mongodb.MongoException ex) {
                    // writeLastTimestamp(lastTimestamp);
                    System.out.println("Internal exception, waiting");
                    Thread.sleep(2000);
                } catch (Exception ex) {
                    killMe = true;
                    // writeLastTimestamp(lastTimestamp);
                    ex.printStackTrace();
                    break;
                }
                Thread.yield();
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

    void report(String collectionName, long count, long skips, long duration) {
        double brate = (double) count / ((duration) / 1000.0);
        logger.debug(collectionName + ": " + count + " records, " + brate + " req/sec, " + skips + " skips, " + pool + " tasks");
    }

}
