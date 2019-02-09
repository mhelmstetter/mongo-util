/*
 * Copyright (c)2006-2009 Mark Logic Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * The use of the Apache License does not indicate that this project is
 * affiliated with the Apache Software Foundation.
 */
package com.mongodb.util;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Michael Blakeley, michael.blakeley@marklogic.com
 * 
 */
public class Monitor extends Thread {
    
    public static final int displayMillis = 15000;
    public static final int sleepMillis = 500;

    protected static final Logger logger = LoggerFactory.getLogger(Monitor.class);

    private volatile Timer timer;

    private static long lastDisplayMillis = 0;

    //private volatile String lastUri;

    private boolean running = true;

    

    private ThreadPoolExecutor pool;

    private int totalSkipped = 0;

    private Thread parent;

    private int lastSkipped = 0;

    private long lastCount = 0;

    @SuppressWarnings("unused")
    private Monitor() {
        // avoid no-argument constructors
    }

    /**
     * @param _c
     * @param _p
     */
    public Monitor(Thread _p) {
        parent = _p;
        lastDisplayMillis = System.currentTimeMillis();
    }

    public void run() {
        logger.debug("starting");

        timer = new Timer();
        try {
            monitor();
            // successful exit
            timer.stop();
            logger.info("replayed " + timer.getSuccessfulEventCount()
                    + " records ok (" + timer.getProgressMessage(true)
                    + "), with " + timer.getErrorCount() + " error(s)");
        } catch (Throwable t) {
            logger.error("fatal error", t);
        } finally {
            cleanup();
        }
        logger.trace("exiting");
    }

    private void cleanup() {
        pool.shutdownNow();

        logger.trace("waiting for pool to terminate");

        try {
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e1) {
        }

        parent.interrupt();

        if (isInterrupted()) {
            logger.info("resetting interrupt status");
            interrupted();
        }
    }

    /**
     * 
     */
    private void monitor() throws Exception {
        
        long currentMillis;

        // if anything goes wrong, the futuretask knows how to stop us
        // hence, we do nothing with the pool in this loop
        logger.trace("looping every " + sleepMillis);
        while (running && !isInterrupted()) {
            // try to avoid thread starvation
            yield();
            
            currentMillis = System.currentTimeMillis();
            long elapsed = currentMillis - lastDisplayMillis;
            //logger.trace("Monitor loop " + currentMillis + " " + lastDisplayMillis + " " + elapsed);
            if (currentMillis - lastDisplayMillis > displayMillis) {
                    //&& (lastSkipped < totalSkipped || lastCount < timer.getEventCount())) {
                lastDisplayMillis = currentMillis;
                lastSkipped = totalSkipped;
                // events include errors
                lastCount = timer.getEventCount();
                logger.info("inserted record " + timer.getEventCount()
                         + " ("
                        + timer.getProgressMessage() + "), with "
                        + timer.getErrorCount() + " error(s) "
                        + pool.getActiveCount() + " active threads");
                logger.trace("thread count: core="
                        + pool.getCorePoolSize() + ", active="
                        + pool.getActiveCount());
                
                
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // interrupt status will be reset below
            }
        }
        if (isInterrupted()) {
            interrupted();
        }
    }

    /**
     * 
     */
    public void halt() {
        if (!running) {
            return;
        }
        logger.info("halting");
        running = false;
        pool.shutdownNow();
        // for quicker shutdown
        interrupt();
    }

    /**
     * 
     */
    public void halt(Throwable t) {
        logger.warn("fatal - halting monitor");
        logger.error(t.getMessage());
        halt();
    }

    /**
     * @param _uri
     * @param _event
     */
    public synchronized void add(TimedEvent _event) {
        timer.add(_event, false);
    }

    

    /**
     * @return
     */
    public long getEventCount() {
        return timer.getEventCount();
    }



    /**
     * 
     */
    public void incrementSkipped(String message) {
        totalSkipped++;
    }

    public ThreadPoolExecutor getPool() {
        return pool;
    }

    public void setPool(ThreadPoolExecutor pool) {
        this.pool = pool;
    }

    /**
     * @param _msg
     */
    public synchronized void resetTimer(String _msg) {
        timer.stop();
        logger.info(_msg + " " + timer.getSuccessfulEventCount()
                + " records ok (" + timer.getProgressMessage(true)
                + "), with " + timer.getErrorCount() + " error(s)");
        timer = new Timer();
    }

}
