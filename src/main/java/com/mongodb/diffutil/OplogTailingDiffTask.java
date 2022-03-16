package com.mongodb.diffutil;

import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.ne;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.model.Namespace;
import com.mongodb.model.OplogSummary;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.oplog.OplogUtil;
import com.mongodb.shardsync.ShardClient;
import com.mongodb.util.DiffUtils;

public class OplogTailingDiffTask implements Callable<OplogTailingDiffTaskResult> {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogTailingDiffUtil.class);
	
	private final static int ONE_MINUTE = 60 * 1000;
	private final static Document SORT_ID = new Document("_id", 1);
	
	private final static int batchSize = 1000;
    
    private String sourceShardId;
    private String destShardId;
    private ShardClient sourceClient;
    private ShardClient destClient;
    
    private MongoClient sourceMongoClient;
    private MongoClient destMongoClient;
    
//    private BlockingQueue<Runnable> workQueue;
//    protected ThreadPoolExecutor executor = null;
//    private ExecutorCompletionService<DiffResult> completionService;
//    private Monitor monitor;
    
    List<Future<DiffResult>> futures =  Collections.synchronizedList(new LinkedList<Future<DiffResult>>());
    
    Map<Namespace, Set<Object>> namespaceBuffers = new HashMap<>();
    
    public OplogTailingDiffTask(String sourceShardId, String destShardId, ShardClient sourceClient, ShardClient destClient, int threads, int queueSize) {
        this.sourceShardId = sourceShardId;
        this.destShardId = destShardId;
        this.sourceClient = sourceClient;
        this.destClient = destClient;
        this.sourceMongoClient = sourceClient.getShardMongoClient(sourceShardId);
        this.destMongoClient = destClient.getShardMongoClient(destShardId);
        
//        workQueue = new LinkedBlockingQueue<Runnable>(queueSize);
//        executor = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, workQueue, new CallerBlocksPolicy(ONE_MINUTE*5));
//        completionService = new ExecutorCompletionService<DiffResult>(executor);
    }
    
    private DiffResult diff(Set<Object> buffer, String sourceShardId, Namespace ns, MongoClient sourceClient, MongoClient destClient) throws Exception {
		//logger.debug("DiffTask.call()");
		
		DiffResult result = new DiffResult(sourceShardId);
		
		Bson filter = in("_id", buffer);
		
		MongoDatabase sourceDb = sourceMongoClient.getDatabase(ns.getDatabaseName());
		MongoDatabase destDb = destMongoClient.getDatabase(ns.getDatabaseName());
		MongoCollection<RawBsonDocument> sourceColl = sourceDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);
		MongoCollection<RawBsonDocument> destColl = destDb.getCollection(ns.getCollectionName(), RawBsonDocument.class);

		MongoCursor<RawBsonDocument> sourceCursor = sourceColl.find(filter).sort(SORT_ID).iterator();
		MongoCursor<RawBsonDocument> destCursor = destColl.find(filter).sort(SORT_ID).iterator();

		RawBsonDocument sourceDoc = null;
		RawBsonDocument destDoc = null;
		byte[] sourceBytes = null;
		byte[] destBytes = null;
		
		Set<Object> sourceIds = new HashSet<>();
		Set<Object> destIds = new HashSet<>();
		
		long lastReport = System.currentTimeMillis();
		while (sourceCursor.hasNext()) {
			sourceDoc = sourceCursor.next();
			Object sourceId = sourceDoc.get("_id");
			sourceIds.add(sourceId);
			if (destCursor.hasNext()) {
				destDoc = destCursor.next();
				Object destId = destDoc.get("_id");
				destIds.add(destId);
			} else {
				logger.error(String.format("%s - destCursor exhausted, doc %s missing", ns, sourceId));
				result.incrementMissing();
				continue;
			}
			sourceBytes = sourceDoc.getByteBuffer().array();
			destBytes = destDoc.getByteBuffer().array();
			if (sourceBytes.length == destBytes.length) {
				if (!DiffUtils.compareHashes(sourceBytes, destBytes)) {
					Object id = sourceDoc.get("_id");

					if (sourceDoc.equals(destDoc)) {
						logger.error(String.format("%s - docs equal, but hash mismatch, id: %s", ns, id));
						result.incrementKeysMisordered();
					} else {
						logger.error(String.format("%s - doc hash mismatch, id: %s", ns, id));
						result.incrementHashMismatched();
					}

				} else {
					//logger.debug("Match! " + sourceId);
					result.incrementMatches();
				}
			} else {
				logger.debug("Doc sizes not equal, id: " + sourceId);
				boolean xx = DiffUtils.compareDocuments(ns.getNamespace(), sourceDoc, destDoc);
				result.incrementHashMismatched();
			}
			result.incrementTotal();
		}
		
		Set<Object> diff = null;
		
		logger.debug(String.format("buffer size: %s, sourceIds: %s", buffer.size(), sourceIds.size()));
		
//		diff = Sets.difference(buffer, sourceIds);
//		if (!diff.isEmpty()) {
//			logger.debug("buffer diff" + diff);
//		}
		
		diff = Sets.difference(sourceIds, destIds);
		if (!diff.isEmpty()) {
			logger.debug("xx    - keys do not match: keys missing from source" + diff);
		}
		

		diff = Sets.difference(destIds, sourceIds);
		if (!diff.isEmpty()) {
			logger.debug("yy    - keys do not match: keys missing from dest" + diff);
		}
			
		
		
		//logger.debug(String.format("%s - call complete, id: %s", sourceShardId, id));
		return result;
	}

    
    @Override
    public OplogTailingDiffTaskResult call() throws Exception {
        
    	ShardTimestamp shardTimestamp = sourceClient.populateLatestOplogTimestamp(sourceShardId, null);
    	
    	OplogTailingDiffTaskResult result = new OplogTailingDiffTaskResult(sourceShardId);
    	
    	MongoDatabase local = sourceClient.getShardMongoClient(sourceShardId).getDatabase("local");
        MongoCollection<RawBsonDocument> oplog = local.getCollection("oplog.rs", RawBsonDocument.class);
        
        MongoCursor<RawBsonDocument> cursor = null;
        
        //Bson query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n"));
        Bson query = ne("op", "n");
        long start = System.currentTimeMillis();
        long totalCount = 0;
        long buffersCount = 0;
        long taskCount = 0;
        try {
            //cursor = oplog.find(query).noCursorTimeout(true).cursorType(CursorType.TailableAwait).iterator();
        	cursor = oplog.find(query).iterator();
            while (cursor.hasNext()) {
                RawBsonDocument doc = cursor.next();
                totalCount++;
                
                OplogSummary oplogSummary = OplogUtil.getOplogSummaryFromOplogEntry(doc);
                if (oplogSummary.getId() != null) {
                	Set<Object> buffer = getBuffer(oplogSummary.getNs());
                	buffer.add(oplogSummary.getId());
                	buffersCount++;
                	
                	if (buffersCount >= batchSize) {
                		
                		//DiffTask diffTask = new DiffTask(Collections.unmodifiableSet(buffer), sourceShardId, oplogSummary.getNs(), sourceMongoClient, destMongoClient, taskCount);
                		//futures.add(executor.submit(diffTask));
                		DiffResult diffResult = diff(buffer, sourceShardId, oplogSummary.getNs(), sourceMongoClient, destMongoClient);
                		result.addDiffResult(diffResult);
                        taskCount++;
                        buffersCount = 0;
                        buffer = new HashSet<>(batchSize);
                        //buffer.clear();
                	}
                }
            }
            
            for (Map.Entry<Namespace, Set<Object>> entry : namespaceBuffers.entrySet()) {
                Namespace ns = entry.getKey();
                Set<Object> buffer = entry.getValue();
                
                //DiffTask diffTask = new DiffTask(Collections.unmodifiableSet(buffer), sourceShardId, ns, sourceMongoClient, destMongoClient, taskCount);
                //futures.add(executor.submit(diffTask));
                DiffResult diffResult = diff(buffer, sourceShardId, ns, sourceMongoClient, destMongoClient);
                result.addDiffResult(diffResult);
                taskCount++;
            }
            
        } catch (MongoException me) {
        	// TODO retry / restart
        	logger.error("oops!", me);
        } finally {
            cursor.close();
        }
        long end = System.currentTimeMillis();
        Double dur = (end - start)/1000.0;
        
//        logger.debug(String.format("%s - starting take() taskCount: %s, totalCount: %s", sourceShardId, taskCount, totalCount));
//        
//        for (Future<DiffResult> future : futures) {
//        	DiffResult diffResult = future.get();
//        	result.addDiffResult(diffResult);
//        }
//        
//        executor.shutdown();
//        
        return result;
    }


	private Set<Object> getBuffer(Namespace ns) {
		Set<Object> buffer = namespaceBuffers.get(ns);
		if (buffer == null) {
			buffer = new HashSet<>();
			namespaceBuffers.put(ns, buffer);
		}
		return buffer;
	}

}
