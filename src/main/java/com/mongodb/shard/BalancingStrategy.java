package com.mongodb.shard;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

public abstract class BalancingStrategy {
	
	protected static Logger logger = LoggerFactory.getLogger(Balancer.class);
	
	protected String uri;
	
	protected MongoClient mongoClient;
	
	protected ShardClient shardClient;
	
	protected boolean dryRun;
	
	protected MongoCollection<Document> chunks;
	
	public BalancingStrategy(String uri) {
		shardClient = new ShardClient("source", uri);
		shardClient.init();
		shardClient.stopBalancer();
		mongoClient = shardClient.getMongoClient();
		
		MongoDatabase db = mongoClient.getDatabase("config");
		chunks = db.getCollection("chunks");
	}
	
	public abstract void balance();

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

}
