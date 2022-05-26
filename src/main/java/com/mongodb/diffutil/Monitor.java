package com.mongodb.diffutil;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.util.Timer;


public class Monitor extends Thread {
    
    public static final int displayMillis = 15000;
    public static final int sleepMillis = 500;

    protected static final Logger logger = LoggerFactory.getLogger(Monitor.class);

    private volatile Timer timer;

    private static long lastDisplayMillis = 0;

    private boolean running = true;

    

    private ThreadPoolExecutor pool;

    private int totalSkipped = 0;

    private Thread parent;

    private int lastSkipped = 0;

    private long lastCount = 0;

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
            logger.info("replayed " + timer.getEventCount()
                    + " records ok (" + timer.getProgressMessage()
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

        logger.trace("waiting for executor to terminate");

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
        // hence, we do nothing with the executor in this loop
        int count = 0;
        logger.trace("looping every " + sleepMillis);
        while (running && !isInterrupted()) {
            // try to avoid thread starvation
            Thread.yield();
            
            currentMillis = System.currentTimeMillis();
            long elapsed = currentMillis - lastDisplayMillis;
            //logger.trace("Monitor loop " + currentMillis + " " + lastDisplayMillis + " " + elapsed);
            if (currentMillis - lastDisplayMillis > displayMillis) {
                    //&& (lastSkipped < totalSkipped || lastCount < timer.getEventCount())) {
                lastDisplayMillis = currentMillis;
                lastSkipped = totalSkipped;
                // events include errors
                lastCount = timer.getEventCount().get();
                logger.info("replayed record " + timer.getEventCount()
                         + " ("
                        + timer.getProgressMessage() + "), with "
                        + timer.getErrorCount() + " error(s) "
                        + pool.getActiveCount() + " active threads, " + pool.getQueue().size() + " queued tasks");
                logger.info("thread count: core="
                        + pool.getCorePoolSize() + ", active="
                        + pool.getActiveCount());
                
                
            }

            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // interrupt status will be reset below
            }
            count++;
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

//    /**
//     * @param _uri
//     * @param _event
//     */
//    public synchronized void add(TimedEvent _event) {
//        timer.add(_event, false);
//    }

    public void incrementEventCount() {
        timer.incrementEventCount();
    }
    
    public void incrementErrorCount() {
        timer.incrementErrorCount();
    }
    


    /**
     * @return
     */
    public long getEventCount() {
        return timer.getEventCount().get();
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

//    /**
//     * @param _msg
//     */
//    public synchronized void resetTimer(String _msg) {
//        timer.stop();
//        logger.info(_msg + " " + timer.getSuccessfulEventCount()
//                + " records ok (" + timer.getProgressMessage(true)
//                + "), with " + timer.getErrorCount() + " error(s)");
//        timer = new Timer();
//    }

}
