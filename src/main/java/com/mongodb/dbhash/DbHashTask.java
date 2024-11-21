package com.mongodb.dbhash;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.dbhash.model.DbHashResult;
import com.mongodb.dbhash.model.ShardDatabasePair;

public class DbHashTask implements Callable<DbHashResult> {
	
	protected static final Logger logger = LoggerFactory.getLogger(DbHashTask.class);
	
	private MongoClient client;
	private ShardDatabasePair pair;
	private boolean isSource;
	
	public DbHashTask(MongoClient client, ShardDatabasePair pair, boolean isSource) {
		this.client = client;
		this.pair = pair;
		this.isSource = isSource;
	}

	@Override
	public DbHashResult call() throws Exception {
		Document command = new Document("dbHash", 1);
		List<String> collections = new ArrayList<>();
		command.append("collections", collections);
		collections.add(pair.getNamespace().getCollectionName());
		Document cmdResult = client.getDatabase(pair.getNamespace().getDatabaseName()).runCommand(command);
		String md5 = cmdResult.getString("md5");
		DbHashResult result = new DbHashResult(pair.getNamespace(), md5, isSource);
		if (isSource) {
			logger.debug("source hash: {}, ns: {}", md5, pair.getNamespace());
			result.setSourceShard(pair.getSourceShard());
		} else {
			logger.debug("dest hash: {}, ns: {}", md5, pair.getNamespace());
			result.setDestShard(pair.getDestinationShard());
		}
		return result;
	}
	


}
