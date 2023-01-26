package com.mongodb.stats;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientSettings;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.internal.MongoClientImpl;
import com.mongodb.model.Collection;
import com.mongodb.model.CollectionStats;
import com.mongodb.model.DatabaseCatalog;
import com.mongodb.model.DatabaseCatalogProvider;
import com.mongodb.model.StandardDatabaseCatalogProvider;
import com.mongodb.shardsync.ShardClient;

public class StatsUtil {
    
    private static Logger logger = LoggerFactory.getLogger(StatsUtil.class);
    
    private String mongoUri;
    private ShardClient shardClient;
    private MongoCollection<Document> shardStats;
    
    public void setMongoUri(String mongoUri) {
        this.mongoUri = mongoUri;
    }

    public void init() {
    	shardClient = new ShardClient("statsClient", mongoUri);
    	shardClient.init();
    	MongoDatabase db = shardClient.getMongoClient().getDatabase("Diff3");
    	shardStats = db.getCollection("shardStats");
    }

	public void stats() {
		
		shardClient.populateShardMongoClients();
		Map<String, MongoClient> shardsMap = shardClient.getShardMongoClients();
		
		for (Map.Entry<String, MongoClient> entry : shardsMap.entrySet()) {
			String shardId = entry.getKey();
			MongoClientImpl mc = (MongoClientImpl)entry.getValue();
			MongoClientSettings mcs = mc.getSettings();
			
			List<ServerAddress> hosts = mcs.getClusterSettings().getHosts();
			for (ServerAddress sa : hosts) {
				logger.debug(entry.getKey() + " " + sa.getHost());
				
				MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();
				settingsBuilder.applyToClusterSettings(builder -> builder.hosts(Arrays.asList(new ServerAddress[] {sa})));
				settingsBuilder.applyToSslSettings(builder -> builder.enabled(mcs.getSslSettings().isEnabled()));
				settingsBuilder.credential(mcs.getCredential());
				MongoClientSettings settings = settingsBuilder.build();
				MongoClient mongoClient = MongoClients.create(settings);
				
				DatabaseCatalogProvider databaseCatalogProvider = new StandardDatabaseCatalogProvider(mongoClient);
				
				//DatabaseCatalog catalog = databaseCatalogProvider.get(Arrays.asList(new Namespace("CvsCrmMessaging", "MessageEvents")));
				
				DatabaseCatalog catalog = databaseCatalogProvider.get();
				
				Set<Collection> colls = catalog.getUnshardedCollections();
				for (Collection c : colls) {
					CollectionStats cs = c.getCollectionStats();
					
					Document stats = new Document();
					stats.append("host", sa.getHost());
					stats.append("shardId", shardId);
					stats.append("ns", c.getNamespace().getNamespace());
					stats.append("count", cs.getCount());
					stats.append("freeStorageSize", cs.getFreeStorageSize());
					stats.append("numIndexes", cs.getNumIndexes());
					stats.append("size", cs.getSize());
					stats.append("storageSize", cs.getStorageSize());
					stats.append("totalIndexSize", cs.getTotalIndexSize());
					stats.append("totalSize", cs.getTotalSize());
					
					shardStats.insertOne(stats);
				}
				
			}
		}
		
		
		
		
		
	}
    
    

}
