package com.mongodb.shardbalancer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Projections.include;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.shardsync.ShardClient;

public class TailingOplogAnalyzerWorker implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(TailingOplogAnalyzerWorker.class);
	
	private final static BulkWriteOptions unorderedBulkWriteOptions = new BulkWriteOptions().ordered(false);
	
	private int checkpointIntervalMillis;
	
	ShardClient sourceShardClient;
	MongoClient mongoClient;
	String shardId;
	BalancerConfig config;
	
	Map<String, NavigableMap<String, CountingMegachunk>> chunkMap;
	
	private ChunkUpdateBuffer chunkUpdateBuffer;
	private MongoCursor<RawBsonDocument> cursor = null;
	private MongoCollection<RawBsonDocument> oplog;
	
	private Timer timer;

	AtomicBoolean checkpointDue = new AtomicBoolean();
	
	private Map<String, Document> collectionsMap;
	
	int round = 0;
	

	public TailingOplogAnalyzerWorker(String sourceShardId, BalancerConfig config) {
		this.sourceShardClient = config.getSourceShardClient();
		this.mongoClient = sourceShardClient.getShardMongoClient(sourceShardId);
		this.shardId = sourceShardId;
		this.config = config;
		this.chunkMap = config.getChunkMap();
		this.chunkUpdateBuffer = new ChunkUpdateBuffer(shardId, config);
		this.collectionsMap = sourceShardClient.getCollectionsMap();
		this.checkpointIntervalMillis = config.getCheckpointIntervalMinutes() * 60 * 1000;
		timer = new Timer();
		
		MongoDatabase local = mongoClient.getDatabase("local");
		oplog = local.getCollection("oplog.rs", RawBsonDocument.class);
	}

	@Override
	public void run() {
		startCheckpointTimer();
		while (true) {
			if (config.runAnalyzer()) {
				oplogTail();
			}
			try {
				Thread.sleep(30000);
			} catch (InterruptedException e) {
			}
		}
		
		//logger.debug("{}: oplog analyzer worker exiting", shardId);
	}
	
	
	
	private void oplogTail() {
		BsonTimestamp shardTimestamp = getLatestOplogTimestamp();
		ZonedDateTime zonedDateTime = Instant.ofEpochSecond(shardTimestamp.getTime()).atZone(ZoneOffset.UTC);
		
		logger.debug("{}: oplog analyzer starting round {}, latestOplogTimestamp: {}", shardId, round++, DateTimeFormatter.ISO_DATE_TIME.format(zonedDateTime));
		
		Set<String> includedNamespaces = config.getIncludedNamespaceStrings(); 
		Bson query = null;
		if (includedNamespaces.isEmpty()) {
			query = gte("ts", shardTimestamp);
		} else {
			query = and(gte("ts", shardTimestamp), in("ns", includedNamespaces));
		}
		
		chunkUpdateBuffer.start();
		
		try {
			cursor = oplog.find(query).sort(new Document("$natural", 1)).noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext() && config.runAnalyzer() && !checkpointDue.get()) {
				
				RawBsonDocument doc = cursor.next();
				
				String opType = doc.getString("op").getValue();
				if ( opType.equals("n")) {
					continue;
				}

				String ns = doc.getString("ns").getValue();
				
				if (! collectionsMap.containsKey(ns)) {
					continue;
				}
				
				boolean fromMigrate = doc.getBoolean("fromMigrate", BsonBoolean.FALSE).getValue();
			
	            if (fromMigrate || ns.startsWith("config.") || ns.startsWith("admin.")) {
	            	continue;
	            }
	            
	            BsonString id = (BsonString)getIdForOperation(doc);
	            
	            if (id == null) {
	            	continue;
	            }
	            
	            NavigableMap<String, CountingMegachunk> innerMap = chunkMap.get(ns);
	            Map.Entry<String, CountingMegachunk> entry = innerMap.floorEntry(id.getValue());
	            
	            if (entry != null) {
	            	CountingMegachunk m = entry.getValue();
		            
		            if (! m.getShard().equals(shardId)) {
		            	logger.error("shard for this chunk does not match, id: {}, chunk: {}", id, m);
		            	continue;
		            }
		            
		            m.incrementSeenCount();
		            chunkUpdateBuffer.add(m);
		            
		            //logger.debug("{}: op {}, {} -- {}", ns, opType, id, m);
	            } else {
	            	logger.error("no chunk found for key {}", id.getValue());
	            }
			}
			executeCheckpoint();
			checkpointDue.set(false);

		} catch (MongoInterruptedException e) {
			// ignore
		} catch (Exception e) {
			logger.error("tail error", e);
		} finally {
			try {
				cursor.close();
			} catch (Exception e) {
			}
		}
	}
 	
	private void executeCheckpoint() {
		
		MongoCollection<Document> collection = config.getStatsCollection();
		
		List<WriteModel<Document>> writeModels = chunkUpdateBuffer.getWriteModels();
		
		if (writeModels.isEmpty()) {
			logger.debug("{}: checkpoint has nothing to write", shardId);
			return;
		}
		
		BulkWriteResult bulkWriteResult = null;
		try {
			bulkWriteResult = collection.bulkWrite(writeModels, unorderedBulkWriteOptions);

		} catch (MongoBulkWriteException err) {
			bulkWriteResult = err.getWriteResult();
			logger.error("bulk write errors, insertedCount: {}, errorCount: {}", bulkWriteResult.getInsertedCount(), err.getWriteErrors().size());
		} catch (Exception ex) {
			logger.error("{} unknown error: {}", ex.getMessage(), ex);
		}
		
		logger.debug("{}: checkpoint complete, insertedCount: {}", shardId, bulkWriteResult.getInsertedCount());
		writeModels.clear();
		chunkUpdateBuffer.clear();
	}
	
	private void startCheckpointTimer() {
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
            	checkpointDue.set(true);
            }
        }, checkpointIntervalMillis, checkpointIntervalMillis);

	}
	
    private BsonTimestamp getLatestOplogTimestamp() {
		MongoCollection<Document> coll = mongoClient.getDatabase("local").getCollection("oplog.rs");
		Document doc = null;
		doc = coll.find().comment("getLatestOplogTimestamp").projection(include("ts")).sort(eq("$natural", -1)).first();
		BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
		return ts;
	}
	
	private BsonValue getIdForOperation(BsonDocument operation) throws MongoException {
		String opType = operation.getString("op").getValue();
		switch (opType) {
		case "u":
			BsonDocument o2 = operation.getDocument("o2");
			if (o2 != null) {
				BsonValue id = o2.get("_id");
				if (id != null) {
					return id;
				} else {
					logger.warn("{}: did not find o2._id field for update oplog entry: {}", shardId, operation);
				}
			} else {
				logger.error("{}: did not find o2 field for update oplog entry: {}", shardId, operation);
				return null;
			}
			break;
		case "i":
		case "d":
			BsonDocument oDoc = operation.getDocument("o");
			if (oDoc != null) {
				BsonValue id = oDoc.get("_id");
				if (id != null) {
					return id;
				} else {
					logger.warn("{}: did not find o._id field for insert/delete oplog entry: {}", shardId, operation);
				}
			} else {
				logger.error("{}: did not find o field for insert/delete oplog entry: {}", shardId, operation);
			}
			break;
		}
		return null;
	}

}
