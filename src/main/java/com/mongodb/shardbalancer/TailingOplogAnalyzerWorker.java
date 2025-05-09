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
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.oplog.OplogUtil;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.bson.BsonValueWrapper;

public class TailingOplogAnalyzerWorker implements Runnable {
	
	protected static final Logger logger = LoggerFactory.getLogger(TailingOplogAnalyzerWorker.class);
	
	private final static BulkWriteOptions unorderedBulkWriteOptions = new BulkWriteOptions().ordered(false);
	
	ShardClient sourceShardClient;
	MongoClient mongoClient;
	String shardId;
	BalancerConfig config;
	
	Map<String, NavigableMap<BsonValueWrapper, CountingMegachunk>> chunkMap;
	
	private ChunkUpdateBuffer chunkUpdateBuffer;
	private MongoCursor<RawBsonDocument> cursor = null;
	private MongoCollection<RawBsonDocument> oplog;
	

	AtomicBoolean running = new AtomicBoolean();
	
	AtomicBoolean complete = new AtomicBoolean();
	
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
		
		MongoDatabase local = mongoClient.getDatabase("local");
		oplog = local.getCollection("oplog.rs", RawBsonDocument.class);
	}

	@Override
	public void run() {
		while (true) {
			
			if(running.get()) {
				oplogTail();
			}
			
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
			}
		}
	}
	
	public void start() {
		running.set(true);
	}
	
	public void stop() {
		running.set(false);
		while (!complete.get()) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
		}
	}
	
	
	
	private void oplogTail() {
		complete.set(false);
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
			while (cursor.hasNext() && running.get()) {
				
				RawBsonDocument doc = cursor.next();
				
				String opType = doc.getString("op").getValue();
				if ( opType.equals("n") || opType.equals("d")) {
					continue;
				}

				String ns = doc.getString("ns").getValue();
				
				boolean fromMigrate = doc.getBoolean("fromMigrate", BsonBoolean.FALSE).getValue();
	            if (fromMigrate || ns.startsWith("config.") || ns.startsWith("admin.")) {
	            	continue;
	            }
				
				Document collMeta = collectionsMap.get(ns);
				
				// if there's no collection metadata, it's most likely unsharded
				if (collMeta == null) {
					continue;
				}
				Document shardKeyDoc = (Document)collMeta.get("key");
				Set<String> shardKey = shardKeyDoc.keySet();
				
	            
				BsonValueWrapper id = OplogUtil.getIdForOperation(doc, shardKey, shardId);
	            
	            if (id == null) {
	            	logger.debug("id for operation was null: {}", doc);
	            	continue;
	            }
	            
	            NavigableMap<BsonValueWrapper, CountingMegachunk> innerMap = chunkMap.get(ns);
	            
	            //Map.Entry<String, CountingMegachunk> entry = innerMap.floorEntry(id.getValue());
	            Map.Entry<BsonValueWrapper, CountingMegachunk> entry = innerMap.floorEntry(id);
	            
	            //logger.debug("chunk: {}", entry);
	            
	            if (entry != null) {
	            	CountingMegachunk m = entry.getValue();
		            
		            if (! m.getShard().equals(shardId)) {
		            	logger.error("shard for this chunk does not match, id: {}, chunk: {}, seen on shard: {}, opType: {}", id, m, shardId, opType);
		            	logger.debug("inner map size: {}, ns: {}, lastKey: {}", innerMap.size(), ns, innerMap.lastKey());
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

		} catch (MongoInterruptedException e) {
			logger.warn("interrupted", e);
		} catch (Exception e) {
			logger.error("tail error for shard {}, error: {}", shardId, e.getMessage());
		} finally {
			try {
				cursor.close();
			} catch (Exception e) {
			}
		}
		complete.set(true);
	}
 	
	private void executeCheckpoint() {
		
		MongoCollection<BsonDocument> collection = config.getStatsCollection();
		
		List<WriteModel<BsonDocument>> writeModels = chunkUpdateBuffer.getWriteModels();
		
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
	
    private BsonTimestamp getLatestOplogTimestamp() {
		MongoCollection<Document> coll = mongoClient.getDatabase("local").getCollection("oplog.rs");
		Document doc = null;
		doc = coll.find().comment("getLatestOplogTimestamp").projection(include("ts")).sort(eq("$natural", -1)).first();
		BsonTimestamp ts = (BsonTimestamp) doc.get("ts");
		return ts;
	}
	
}
