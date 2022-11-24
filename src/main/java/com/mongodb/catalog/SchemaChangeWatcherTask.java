package com.mongodb.catalog;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TimerTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.model.Collection;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.shardsync.ShardClient;

import jakarta.mail.MessagingException;

public class SchemaChangeWatcherTask extends TimerTask {

	private static Logger logger = LoggerFactory.getLogger(SchemaChangeWatcherTask.class);

	private ShardClient shardClient;

	private DatabaseCatalog catalog;
	
	private EmailSender emailSender;
	
	private String name;

	public SchemaChangeWatcherTask(String name, String clusterUri, EmailSender emailSender) {

		this.name = name;
		shardClient = new ShardClient(name, clusterUri);

		shardClient.init();
		shardClient.populateCollectionsMap();

		shardClient.populateDatabaseCatalog();
		catalog = shardClient.getDatabaseCatalog();
		this.emailSender = emailSender;
	}

	@Override
	public void run() {

		logger.debug("{}: run started", name);
		shardClient.populateCollectionsMap(true);
		shardClient.populateDatabaseCatalog();
		DatabaseCatalog newCatalog = shardClient.getDatabaseCatalog();

		Set<Collection> newCollections = new LinkedHashSet<>();
		Set<Collection> droppedCollections = new LinkedHashSet<>();
		
		newCollections.addAll(Sets.difference(newCatalog.getShardedCollections(), catalog.getShardedCollections()));
		newCollections.addAll(Sets.difference(newCatalog.getUnshardedCollections(),
				catalog.getUnshardedCollections()));
		
		droppedCollections.addAll(Sets.difference(catalog.getShardedCollections(),
				newCatalog.getShardedCollections()));
		droppedCollections.addAll(Sets.difference(catalog.getUnshardedCollections(),
				newCatalog.getUnshardedCollections()));
		
		
		if (newCollections.size() > 0 || droppedCollections.size() > 0) {
			logger.debug("{}: new collections: {}, dropped collections: {}", name, newCollections, 
					droppedCollections);
			try {
				emailSender.sendReport(name, newCollections, droppedCollections);
			} catch (MessagingException e) {
				logger.error("{}: error sending email", e);
			}
		} else {
			logger.debug("{}: no schema changes found", name);
		}

		catalog = newCatalog;
		logger.debug("{}: run complete", name);
	}

}
