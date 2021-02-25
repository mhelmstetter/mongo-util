package com.mongodb.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlockWhenQueueFull implements RejectedExecutionHandler {
	
	protected static final Logger logger = LoggerFactory.getLogger(BlockWhenQueueFull.class);
	
	private final long maxWait = 10 * 60 * 1000;

	public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
		if (!executor.isShutdown()) {
			try {
				BlockingQueue<Runnable> queue = executor.getQueue();
				if (!queue.offer(r, this.maxWait, TimeUnit.MILLISECONDS)) {
					throw new RejectedExecutionException("Max wait time expired to queue task");
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new RejectedExecutionException("Interrupted", e);
			}
		}
		else {
			throw new RejectedExecutionException("Executor has been shut down");
		}
	}
}
