package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.nin;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Sorts;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.BlockWhenQueueFull;

public class ShardedCollectionCloneWorker extends AbstractCollectionCloneWorker implements Runnable {

	protected ThreadPoolExecutor executor = null;
	private BlockingQueue<Runnable> workQueue;

	List<Future<ChunkCloneResult>> chunkCloneResults;

	public ShardedCollectionCloneWorker(ShardClient sourceShardClient, ShardClient destShardClient,
			MongoSyncOptions options) {
		super(null, sourceShardClient, destShardClient, options);

		workQueue = new ArrayBlockingQueue<Runnable>(1000000);

		executor = new ThreadPoolExecutor(options.getThreads(), options.getThreads(), 30, TimeUnit.SECONDS, workQueue, new BlockWhenQueueFull());
		// new ThreadFactoryBuilder().setNameFormat("shard-clone-worker-%d").setDaemon(true).build()
	}
	
	private boolean excludeDbCheck(String dbName) {
    	return ShardClient.excludedSystemDbs.contains(dbName) || options.excludeDb(dbName);
    }
	
	public void shutdown() {
		logger.debug("ShardedCollectionCloneWorker starting shutdown");
		executor.shutdown();
		try {
			if (!executor.awaitTermination(Duration.ofMinutes(60).toMillis(), TimeUnit.MILLISECONDS)) {
				logger.warn("ShardedCollectionCloneWorker executor did not finish within wait time");
			}
		} catch (InterruptedException e) {
			logger.warn("ShardedCollectionCloneWorker interrupted");
			Thread.currentThread().interrupt();
		}
		logger.debug("ShardedCollectionCloneWorker shutdown complete");
	}

	@Override
	public void run() {

		MongoCollection<Document> chunksColl = destShardClient.getChunksCollection();
		Set<String> includedNamespaces = options.getIncludedNamespaceStrings();
		Set<String> excludedNamespaces = options.getExcludedNamespaceStrings();
		
//		if (!includedNamespaces.isEmpty() || !excludedNamespaces.isEmpty()) {
//			Bson query = null;
//			if (excludedNamespaces.size() > 0 && excludedNamespaces.size() < includedNamespaces.size()) {
//				query = nin("ns", excludedNamespaces);
//			} else {
//				query = in("ns", includedNamespaces);
//			}
//		}
		
		chunkCloneResults = new ArrayList<>();
		
		MongoIterable<String> dbNames = sourceShardClient.listDatabaseNames();
        for (String dbName : dbNames) {
        	
        	if (excludeDbCheck(dbName)) {
        		continue;
        	}
        	
        	if (! options.includeDatabase(dbName)) {
        		continue;
        	}
        	
        	if (options.isDropDestDbs()) {
        		destShardClient.dropDatabase(dbName);
        	}
        	
        	List<String> collectionNames = new ArrayList<>();
            sourceShardClient.listCollectionNames(dbName).into(collectionNames);
            logger.debug("{}: collection count (before filtering): {}", dbName, collectionNames.size());
            for (String collectionName : collectionNames) {
                if (collectionName.equals("system.profile") || collectionName.equals("system.indexes")) {
                    continue;
                }
                
                if (! options.includeCollection(dbName, collectionName)) {
                	continue;
                }
                Namespace ns = new Namespace(dbName, collectionName);
                if (options.excludeNamespace(ns)) {
                	continue;
                }
                cloneCollection(ns);
            }
        }
		

		int i = 0;
		for (Future<ChunkCloneResult> future : chunkCloneResults) {
			try {
				ChunkCloneResult chunkCloneResult = future.get();
				logger.debug("chunk clone complete: {}", chunkCloneResult);
			} catch (InterruptedException e) {
				logger.error("interruped getting chunk clone result", e);
			} catch (ExecutionException e) {
				logger.error("error getting chunk clone result", e);
			}
		}

		shutdown();

		logger.debug("ShardedCollectionCloneWorker complete!");

	}

	private void cloneCollection(Namespace ns) {

		shardCollection = sourceShardClient.getCollectionsMap().get(ns.getNamespace());
		if (shardCollection == null) {
			ChunkCloneTask task = new ChunkCloneTask(ns, sourceShardClient, destShardClient, null, options);
			chunkCloneResults.add(executor.submit(task));
		} else {
			Document shardKeysDoc = (Document) shardCollection.get("key");
			Set<String> shardKeys = shardKeysDoc.keySet();

			// use dest chunks as reference, may be smaller
			MongoCollection<Document> chunksCollection = destShardClient.getChunksCollection();
			// int chunkCount = (int)sourceChunksColl.countDocuments(eq("ns",
			// ns.getNamespace()));

			FindIterable<Document> sourceChunks = chunksCollection.find(eq("ns", ns.getNamespace()))
					.sort(Sorts.ascending("min"));
			for (Document sourceChunk : sourceChunks) {
				String id = sourceChunk.getString("_id");
				// each chunk is inclusive of min and exclusive of max
				Document min = (Document) sourceChunk.get("min");
				Document max = (Document) sourceChunk.get("max");
				Bson chunkQuery = null;

				if (shardKeys.size() > 1) {
					List<Bson> filters = new ArrayList<Bson>(shardKeys.size());
					for (String key : shardKeys) {
						filters.add(and(gte(key, min.get(key)), lt(key, max.get(key))));
					}
					chunkQuery = and(filters);
				} else {
					String key = shardKeys.iterator().next();
					chunkQuery = and(gte(key, min.get(key)), lt(key, max.get(key)));
				}

				ChunkCloneTask task = new ChunkCloneTask(ns, sourceShardClient, destShardClient, chunkQuery, options);
				chunkCloneResults.add(executor.submit(task));
			}
		}
	}

}
