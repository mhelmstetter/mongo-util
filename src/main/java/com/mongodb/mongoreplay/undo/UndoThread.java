package com.mongodb.mongoreplay.undo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.mongodb.client.MongoClient;

public class UndoThread extends Thread {

    BlockingQueue<Object> inputQueue;
    
    private MongoClient mongoClient;

    public UndoThread(BlockingQueue<Object> inputQueue, MongoClient mongoClient) {
        this.inputQueue = inputQueue;
        this.mongoClient = mongoClient;
    }

    public void run() {
        while (inputQueue.size() > 0) {
            Object undoOp = null;
            try {
                undoOp = inputQueue.poll(1, TimeUnit.SECONDS);
                System.out.println("**** got unodo op " + undoOp);
                if (undoOp == null)
                    continue;
            } catch (InterruptedException interrupted) {
                // _logger.warn("Unexpectedly interrupted while polling for
                // blocks.", interrupted);
                return;
            }

            // _livingBlocks.addElement(blockHash);
        }

        System.out.println("done?");
    }
}
