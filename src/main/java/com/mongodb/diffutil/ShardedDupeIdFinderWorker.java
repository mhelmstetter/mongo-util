package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lte;

import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;

public class ShardedDupeIdFinderWorker implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ShardedDupeIdFinderWorker.class);

	private String sourceColl;
	private String dbName;

	Set<BsonValue> idsSet;

	Document sort = new Document("_id", 1);
	private String shardName;

	private Date startDate;
	private Date endDate;

	private ShardClient sourceShardClient;

	public ShardedDupeIdFinderWorker(ShardClient sourceShardClient, String dbName, String sourceColl, Date startDate,
			Date endDate) {
		this.sourceShardClient = sourceShardClient;
		this.dbName = dbName;
		this.sourceColl = sourceColl;
		this.startDate = startDate;
		this.endDate = endDate;
		this.idsSet = new HashSet<>();
	}
	
	private static int dateToTimestampSeconds(final Date time) {
        return (int) (time.getTime() / 1000);
    }

	private ObjectId getObjectId(Date d) {
		int ts = dateToTimestampSeconds(d);

		String hex = Integer.toHexString(ts);

		ObjectId id2 = new ObjectId(hex + "0000000000000000");
		return id2;
	}

	@Override
	public void run() {
		long start = System.currentTimeMillis();
		long count = 0;
		
		ObjectId startObj = getObjectId(startDate);
        ObjectId endObj = getObjectId(endDate);
        
        Bson query = and(gte("_id", startObj), lte("_id", endObj));
		
		for (Map.Entry<String, MongoClient> entry : sourceShardClient.getShardMongoClients().entrySet()) {
            MongoClient sourceClient = entry.getValue();
            String shardName = entry.getKey();
            
            MongoDatabase db = sourceClient.getDatabase(dbName);
            MongoCollection<RawBsonDocument> collection = db.getCollection(sourceColl, RawBsonDocument.class);
            
            MongoCursor<RawBsonDocument> sourceCursor = null;
    		try {
    			sourceCursor = collection.find(query).projection(sort).iterator();
    			while (sourceCursor.hasNext()) {
    				count++;
    				RawBsonDocument doc = sourceCursor.next();
    				BsonValue id = null;
    				try {
    					id = doc.get("_id");
    				} catch (Exception e) {
    					logger.debug(String.format("%s: %s - Error reading doc id, count: %s, error: %s", shardName,
    							collection.getNamespace(), count, e));
    					continue;
    				}
    				boolean newEntry = idsSet.add(id);
    				if (!newEntry) {
    					logger.warn(String.format("%s: dupe key for %s", shardName, id));
    				}

    				if (count % 1000000 == 0) {
    					logger.debug(String.format("%s: read %s", shardName, count));
    				}
    			}

    		} finally {
    			sourceCursor.close();

    		}
		}
		
		long end = System.currentTimeMillis();
		Double dur = (end - start) / 1000.0;
		logger.debug(
				String.format("Done validating %s, %s documents in %f seconds (%s - %s)", sourceColl, count, dur, startDate, endDate));

	}

}
