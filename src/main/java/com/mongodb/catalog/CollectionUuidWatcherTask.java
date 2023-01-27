package com.mongodb.catalog;

import java.util.List;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.shardsync.ShardConfigSync;

import jakarta.mail.MessagingException;

public class CollectionUuidWatcherTask extends TimerTask {

	private static Logger logger = LoggerFactory.getLogger(CollectionUuidWatcherTask.class);

	private EmailSender emailSender;
	
	private String name;
	
	private ShardConfigSync sync;

	public CollectionUuidWatcherTask(String name, ShardConfigSync sync, EmailSender emailSender) {
		
		this.name = name;
		this.sync = sync;
		this.emailSender = emailSender;
	}

	@Override
	public void run() {

		logger.debug("{}: {} run started", name, this.getClass().getSimpleName());
		
		List<String> failures = sync.compareCollectionUuids();
		if (failures != null && failures.size() > 0) {
			try {
				emailSender.sendUuidFailureReport(name, failures);
			} catch (MessagingException e) {
				logger.error("{}: error sending email", e);
			}
		}
		
		logger.debug("{}: {} run complete", name, this.getClass().getSimpleName());
	}

}
