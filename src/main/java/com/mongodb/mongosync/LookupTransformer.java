package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.eq;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class LookupTransformer {
	
	private MongoCollection<BsonDocument> coll;
	private String lookupValueKey;
	
	LoadingCache<ObjectId, BsonValue> cache;
	
	public class DbCacheLoader extends CacheLoader<ObjectId, BsonValue> {

		@Override
		public BsonValue load(ObjectId key) throws Exception {
			BsonDocument doc = coll.find(eq("_id", key)).projection(eq(lookupValueKey, 1)).first();
			if (doc != null) {
				return doc.get(lookupValueKey);
			}
			return null;
		}
	}
	
	
	public LookupTransformer(MongoClient client, String dbName, String collName, String lookupValueKey) {
		this.lookupValueKey = lookupValueKey;
		MongoDatabase db = client.getDatabase(dbName);
		this.coll = db.getCollection(collName, BsonDocument.class);
		this.cache = CacheBuilder.newBuilder()
				  .maximumSize(1000000)
				  .build(new DbCacheLoader());
	}
	
	public BsonValue lookup(ObjectId id) throws ExecutionException {
		return cache.get(id);
	}

	public String getLookupValueKey() {
		return lookupValueKey;
	}

}
