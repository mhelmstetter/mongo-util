package com.mongodb.oplog.analyzer;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonDateTime;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoInterruptedException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.shardsync.ShardClient;
import com.opencsv.CSVWriter;

public class OplogAnalyzerWorker implements Runnable {

	protected static final Logger logger = LoggerFactory.getLogger(OplogAnalyzerWorker.class);

	DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;

	private boolean shutdown = false;
	private ShardClient sourceShardClient;
	private String shardId;
	private Map<OplogEntryKey, EntryAccumulator> accumulators = new HashMap<OplogEntryKey, EntryAccumulator>();

	private CSVWriter writer;

	public OplogAnalyzerWorker(ShardClient sourceShardClient, String shardId) {
		this.sourceShardClient = sourceShardClient;
		this.shardId = shardId;

		try {
			writer = new CSVWriter(new FileWriter("oplog_" + shardId + ".csv"));
			String[] header = {"date","shard","ns","op","count","minSizeBytes","maxSizeBytes", "totalSizeBytes"};
			writer.writeNext(header);
		} catch (IOException e) {
			logger.error("error writing csv", e);
		}

	}

	@Override
	public void run() {

		BsonTimestamp shardTimestamp = sourceShardClient.getLatestOplogTimestamp(shardId);
		Bson query = and(gte("ts", shardTimestamp), ne("op", "n"));

		logger.debug("worker {}: started, start timestamp: {}", shardId, shardTimestamp);

		MongoDatabase local = sourceShardClient.getShardMongoClient(shardId).getDatabase("local");
		MongoCursor<RawBsonDocument> cursor = null;
		MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);

		String lastCheckpoint = null;
		try {
			cursor = oplog.find(query).sort(new Document("$natural", 1)).noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext() && !shutdown) {

				RawBsonDocument doc = cursor.next();

				String ns = ((BsonString) doc.get("ns")).getValue();
				BsonString op = (BsonString) doc.get("op");
				String opType = op.getValue();
				BsonDateTime date = doc.getDateTime("wall");
				long dateLong = date.getValue()/1000;
				LocalDateTime dt = LocalDateTime.ofEpochSecond(dateLong, 0, ZoneOffset.UTC);
				dt = dt.withSecond(0).withNano(0).plusMinutes((65 - dt.getMinute()) % 5);
				String dateStr = formatter.format(dt);
				
				if (lastCheckpoint != null && !dateStr.equals(lastCheckpoint)) {
					flushCsv();
				}

				// ignore no-op
				if (ns.startsWith("config.")) {
					continue;
				}

				OplogEntryKey key = new OplogEntryKey(ns, opType, dateStr);
				EntryAccumulator accum = accumulators.get(key);
				if (accum == null) {
					accum = new EntryAccumulator(key);
					accumulators.put(key, accum);
				}

				long len = doc.getByteBuffer().asNIO().array().length;
				accum.addExecution(len);

				lastCheckpoint = dateStr;
			}

		} catch (MongoInterruptedException e) {
			// ignore
		} catch (Exception e) {
			logger.error("{}: tail error", shardId, e);
		} finally {
			try {
				cursor.close();
			} catch (Exception e) {
			}
		}

	}

	public void flushCsv() {

		try {

			for (EntryAccumulator acc : accumulators.values()) {
				writer.writeNext(new String[] { acc.getDate(), shardId, acc.getNamespace(), acc.getOp(),
						String.valueOf(acc.getCount()), String.valueOf(acc.getMin()), String.valueOf(acc.getMax()),
						String.valueOf(acc.getTotal()) });
			}
			writer.flush();
			accumulators.clear();

		} catch (IOException e) {
			logger.error("error writing csv", e);
		}
	}

	protected synchronized void stop() {
		logger.debug("{} received stop() request", shardId);
		shutdown = true;
		flushCsv();
	}

	public Map<OplogEntryKey, EntryAccumulator> getAccumulators() {
		return accumulators;
	}

	public String getShardId() {
		return shardId;
	}

}
