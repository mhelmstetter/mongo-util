package com.mongodb.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A light wrapper around the {@link ThreadPoolExecutor}. It allows for you to pause execution and
 * resume execution when ready. It is very handy for games that need to pause.
 *
 * @author Matthew A. Johnston (warmwaffles)
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {
    
    protected static final Logger logger = LoggerFactory.getLogger(PausableThreadPoolExecutor.class);
    
    private boolean isPaused;
    private ReentrantLock lock;
    private Condition condition;

    /**
     * @param corePoolSize    The size of the executor
     * @param maximumPoolSize The maximum size of the executor
     * @param keepAliveTime   The amount of time you wish to keep a single task alive
     * @param unit            The unit of time that the keep alive time represents
     * @param workQueue       The queue that holds your tasks
     * @see {@link ThreadPoolExecutor#ThreadPoolExecutor(int, int, long, TimeUnit, BlockingQueue)}
     */
    public PausableThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }
    
    public PausableThreadPoolExecutor(int corePoolSize,
            int maximumPoolSize,
            long keepAliveTime,
            TimeUnit unit,
            BlockingQueue<Runnable> workQueue,
            RejectedExecutionHandler handler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }

    /**
     * @param thread   The thread being executed
     * @param runnable The runnable task
     * @see {@link ThreadPoolExecutor#beforeExecute(Thread, Runnable)}
     */
    @Override
    protected void beforeExecute(Thread thread, Runnable runnable) {
        super.beforeExecute(thread, runnable);
        lock.lock();
        try {
            while (isPaused) condition.await();
        } catch (InterruptedException ie) {
            thread.interrupt();
        } finally {
            lock.unlock();
        }
    }

    public boolean isRunning() {
        return !isPaused;
    }

    public boolean isPaused() {
        return isPaused;
    }

    /**
     * Pause the execution
     */
    public void pause() {
        lock.lock();
        try {
            isPaused = true;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Resume executor execution
     */
    public void resume() {
        lock.lock();
        try {
            isPaused = false;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}