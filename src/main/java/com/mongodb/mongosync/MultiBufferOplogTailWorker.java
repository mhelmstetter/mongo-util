package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.conversions.Bson;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.mongodb.CursorType;
import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

/**
 * This is the "parent" worker, 1 worker per shard. It will spawn child workers
 * working on a subset of oplog entries by collection/_id hash
 *
 */
public class MultiBufferOplogTailWorker extends AbstractOplogTailWorker implements Runnable {

//	Map<String, Map<Integer, List<BsonDocument>>> buffers;
//	
//	private Map<Integer, ArrayBlockingQueue<Runnable>> workQueues;
//	Map<Integer, ThreadPoolExecutor> executors;
	private int numChildWorkers;

	private Map<Integer, ChildOplogWorker> childWorkers;
	private List<ExecutorService> childExecutors;
	// private ArrayBlockingQueue<BsonDocument> childQueues;
	
	private Map<Integer, BlockingQueue<BsonDocument>> childQueues;

	public MultiBufferOplogTailWorker(ShardTimestamp shardTimestamp, TimestampFile timestampFile,
			ShardClient sourceShardClient, ShardClient destShardClient, MongoSyncOptions options) throws IOException {
		this.shardId = shardTimestamp.getShardName();
		this.shardTimestamp = shardTimestamp;
		this.sourceShardClient = sourceShardClient;
		this.destShardClient = destShardClient;
		this.options = options;
		this.numChildWorkers = options.getOplogThreads();

		childWorkers = new HashMap<>(numChildWorkers);
		childExecutors = new ArrayList<>(numChildWorkers);
		childQueues = new HashMap<>(numChildWorkers);
		
		// childQueues = new ArrayBlockingQueue<>(options.getOplogQueueSize());

		oplogTailMonitor = new OplogTailMonitor(timestampFile, sourceShardClient, childQueues);
		this.applyOperationsHelper = new ApplyOperationsHelper(shardId, oplogTailMonitor, destShardClient);
	}

	private void startChildExecutors() {
		for (int i = 0; i < numChildWorkers; i++) {
			BlockingQueue<BsonDocument> childQueue = new LinkedBlockingQueue<>(options.getOplogQueueSize());
			childQueues.put(i, childQueue);
			ChildOplogWorker worker = new ChildOplogWorker(shardId, childQueue, applyOperationsHelper, oplogTailMonitor, options);
			childWorkers.put(i, worker);
			ExecutorService executor = Executors.newFixedThreadPool(1,  new ThreadFactoryBuilder()
					.setNameFormat("child-oplog-worker_" + shardId + "_" + i).setDaemon(true).build());
			childExecutors.add(executor);
			executor.execute(worker);
		}
	}

	private void stopChildExecutors() {
		
		for (ChildOplogWorker worker : childWorkers.values()) {
			logger.debug("{}: final flush()", shardId);
			worker.flush(0);
		}
		
		
		for (ExecutorService executor : childExecutors) {
			executor.shutdown();
			while (!executor.isTerminated()) {
				try {
					Thread.sleep(10000);
				} catch (InterruptedException e) {
				}
			}
		}
		logger.debug("childExecutors shutdown");
	}

	public void run() {
		startChildExecutors();
		ScheduledExecutorService monitorExecutor = Executors.newScheduledThreadPool(1);
		monitorExecutor.scheduleAtFixedRate(oplogTailMonitor, 0L, 30L, TimeUnit.SECONDS);

		MongoDatabase local = sourceShardClient.getShardMongoClient(shardId).getDatabase("local");
		MongoCollection<BsonDocument> oplog = local.getCollection("oplog.rs", BsonDocument.class);

		MongoCursor<BsonDocument> cursor = null;
		
		Set<String> includedNamespaces = options.getIncludedNamespaceStrings();
		Set<String> excludedNamespaces = options.getExcludedNamespaceStrings();
		
		Bson query = null;
		if (includedNamespaces.isEmpty() && excludedNamespaces.isEmpty()) {
			query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"));
		} else {
			if (excludedNamespaces.size() > 0) {
				query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"), nin("ns", excludedNamespaces));
			} else {
				query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"), in("ns", includedNamespaces));
			}
		}

		long start = System.currentTimeMillis();
		long count;
		try {

			logger.debug("{}: starting oplog tail query: {}", shardId, query);
			count = 0;
			cursor = oplog.find(query).sort(eq("$natural", 1)).oplogReplay(true).noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext() && !shutdown) {
				BsonDocument doc = cursor.next();
				String op = doc.getString("op").getValue();

				if (op.equals("n") || op.equals("c")) {
					continue;
				}
				currentNs = doc.getString("ns").getValue();
				if (! options.includeNamespace(currentNs)) {
					continue;
				}
				
				if (currentNs == null || currentNs.equals("") || currentNs.startsWith("config.")) {
					continue;
				}

				addToBuffer(doc);
				count++;
			}

			long end = System.currentTimeMillis();
			Double dur = (end - start) / 1000.0;
			logger.debug("{}: oplog tail complete, {} operations applied in {} seconds", shardId, count, dur);

		} catch (Exception e) {
			logger.error("{}: tail error", shardId, e);
		} finally {
			try {
				cursor.close();
			} catch (Exception e) {
			}
			
		}
		stopChildExecutors();

	}
	
	private int getCombinedHashModulo(String ns, BsonValue id) {
		int hash = 7;
		hash = 31 * hash +  (id == null ? 0 : id.hashCode());
		hash = 31 * hash + (ns == null ? 0 : ns.hashCode());
		if (hash >= 0) {
			hash = hash % numChildWorkers;
		} else {
			hash = -hash % numChildWorkers;
		}
		return hash;
	}

	protected void addToBuffer(BsonDocument doc) throws InterruptedException {
		if (currentNs.endsWith(".$cmd")) {
			logger.debug("$cmd: {}", doc);
		}
		BsonValue id = getIdForOperation(doc);
		Integer hashKey = getCombinedHashModulo(currentNs, id);
		BlockingQueue<BsonDocument> childQueue = childQueues.get(hashKey);
		//logger.debug("{}: child queue {} size {}", shardId, hashKey, childQueue.size());
		//childQueue.put(doc);
		//boolean inserted = childQueue.offer(doc, 5, TimeUnit.SECONDS);
		childQueue.put(doc);
		if (childQueue.size() >= 5000) {
			ChildOplogWorker worker = childWorkers.get(hashKey);
			//logger.debug("{}: forcing worker flush()", shardId);
			worker.flush(5000);
		}
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
		default:
			logger.error(String.format("{}: unexpected operation %s; op: %s", shardId, opType, operation.toJson()));
		}
		return null;
	}

	private Integer getModuloKeyForBsonValue(BsonValue val) {
		Integer key = null;
		int hash = val.hashCode();
		if (hash >= 0) {
			key = hash % numChildWorkers;
		} else {
			key = -hash % numChildWorkers;
		}
		return key;
	}

	private Integer getModuloKeyForNamespace(String ns) {
		Integer key = null;
		int hash = ns.hashCode();
		if (hash >= 0) {
			key = hash % numChildWorkers;
		} else {
			key = -hash % numChildWorkers;
		}
		return key;
	}

	@Override
	protected synchronized void stop() {
		logger.debug("{}: OplogTailWorker got stop signal", shardId);
		this.shutdown = true;
		
		for (ChildOplogWorker worker : childWorkers.values()) {
			worker.stop();
		}

		int i = 0;
		for (ExecutorService executor : childExecutors) {
			try {
				logger.debug("{}: starting shutdown of executor {}", shardId, i++);
				executor.shutdown();
				executor.awaitTermination(60, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.error("Timed out waiting for executor to complete", e);
			}
		}
		logger.debug("{}: executors shutdown", shardId);

	}


}
