package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.eq;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "PartitionForge", mixinStandardHelpOptions = true, version = "PartitionForge 0.1")
public class PartitionForge implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(PartitionForge.class);

	@Option(names = { "--source" }, description = "source mongodb uri connection string", required = true)
	private String sourceUri;

	@Option(names = { "--dest" }, description = "source mongodb uri connection string", required = true)
	private String destUri;

	@Option(names = { "--ns" }, description = "source namespace", required = true)
	private String namespaceStr;

	int sampleCountPerShard = 450;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;
	
	MongoDatabase destDb;
	MongoCollection<Document> partitionColl;
	MongoCollection<Document> resumeDataColl;
	Bson idProj = Projections.fields(Projections.include("_id"));
	
	SortedSet<Object> idSet = new TreeSet<>();

	public void init() {
		sourceShardClient = new ShardClient("source", sourceUri);
		sourceShardClient.init();
		sourceShardClient.populateShardMongoClients();

		destShardClient = new ShardClient("dest", destUri);
		destShardClient.init();
		destDb = destShardClient.getMongoClient().getDatabase("mongosync_reserved_for_internal_use");
		partitionColl = destDb.getCollection("partitions");
		resumeDataColl = destDb.getCollection("resumeData");
		
		long partCount = partitionColl.estimatedDocumentCount();
		if (partCount > 0) {
			logger.debug("{} partitions already exist in mongosync internal db, deleting them", partCount);
			partitionColl.deleteMany(new Document());
		}
	}

	@Override
	public Integer call() throws InterruptedException {

		init();
		long start = System.currentTimeMillis();
		Namespace ns = new Namespace(namespaceStr);
		List<Bson> pipeline = getAggPipeline(sampleCountPerShard);

		// Populate idSet with the cluster level min and max
		MongoCollection<Document> c = sourceShardClient.getCollection(ns);
		Object min = c.find().projection(idProj).sort(Sorts.ascending("_id")).limit(1).first().get("_id");
		logger.debug("min _id: {}", min);
		Object max = c.find().projection(idProj).sort(Sorts.descending("_id")).limit(1).first().get("_id");
		logger.debug("max _id: {}", max);
		idSet.add(min);
		idSet.add(max);

		MongoCollection<Document> collectionsColl = sourceShardClient.getCollection("config.collections");
		Document collectionsDoc = collectionsColl.find(eq("_id", ns.getNamespace())).first();
		Object uuid = collectionsDoc.get("uuid");

		for (MongoClient client : sourceShardClient.getShardMongoClients().values()) {
			MongoCollection<Document> coll = client.getDatabase(ns.getDatabaseName())
					.getCollection(ns.getCollectionName());
			populateIdSet(pipeline, coll);
		}
		logger.debug("idSet size: {}", idSet.size());
		
		// check if set is odd size, keep adding elements until it reaches even size
		pipeline = getAggPipeline(1);
		while ( (idSet.size() % 2) != 0 ) {
			logger.debug("idSet size {} is odd, adding elements until even", idSet.size());
			MongoClient client = sourceShardClient.getShardMongoClients().values().iterator().next();
			MongoCollection<Document> coll = client.getDatabase(ns.getDatabaseName())
					.getCollection(ns.getCollectionName());
			populateIdSet(pipeline, coll);
		}

		Set<String> shards = sourceShardClient.getShardMongoClients().keySet();
		Iterator<String> shardIterator = shards.iterator();

		int nowSecond = (int) Instant.now().getEpochSecond();
		BsonTimestamp ts = new BsonTimestamp(nowSecond, 1);
		List<Document> batch = new ArrayList<>(1000);
		Object previous = null;
		int count = 0;

		// Iterate over idSet and assign each to a shard
		for (Object id : idSet) {

			if (previous != null) {
				Object lowerBound = previous;
				Object upperBound = id;
				
				//logger.debug("low: {}, upper: {}", lowerBound, upperBound);

				// If the iterator has no more elements, reset it
				if (!shardIterator.hasNext()) {
					shardIterator = shards.iterator();
				}

				// Assign the current id to the next shard
				String shard = shardIterator.next();

				Document partitionId = new Document("srcUUID", uuid);
				partitionId.append("id", shard);
				partitionId.append("lowerBound", lowerBound);
				Document doc = new Document("_id", partitionId);
				Document namespace = new Document("db", ns.getDatabaseName());
				namespace.append("coll", ns.getCollectionName());
				doc.append("namespace", namespace);
				doc.append("partitionPhase", "not started");
				doc.append("upperBound", upperBound);
				doc.append("isCapped", false);
				// TODO figure out if we need to populate startedAtTs, finishedAtTs
				doc.append("startedAtTs", ts);
				doc.append("finishedAtTs", ts);
				
				batch.add(doc);
				count++;
				if (batch.size() >= 1000) {
					partitionColl.insertMany(batch);
                    batch.clear();
                }

			}
			previous = id;
		}
		
		if (!batch.isEmpty()) {
			partitionColl.insertMany(batch);
        }
		
		logger.debug("{} partitions created", count);
		
		UpdateResult result = resumeDataColl.updateOne(new Document(), Updates.set("syncPhase", "collection copy"));
		logger.debug("updated resumeData syncPhase, result: {}", result);

		long end = System.currentTimeMillis();
		Double dur = (end - start) / 1000.0;
		logger.debug(String.format("Completed in %f seconds", dur));

		return 0;
	}
	
	private void populateIdSet(List<Bson> pipeline, MongoCollection<Document> coll) {
		AggregateIterable<Document> results = coll.aggregate(pipeline);
		for (Document result : results) {
			Object id = result.get("_id");
			idSet.add(id);
		}
		
	}

	private List<Bson> getAggPipeline(int sampleSize) {
		List<Bson> pipeline = new ArrayList<>();
		pipeline.add(Aggregates.project(idProj));
		pipeline.add(Aggregates.sample(sampleCountPerShard));
		//pipeline.add(Aggregates.sort(Sorts.orderBy(Sorts.ascending("_id"))));
		return pipeline;
	}

	public static void main(String... args) {
		PartitionForge forge = new PartitionForge();
		int exitCode = new CommandLine(forge).execute(args);
		System.exit(exitCode);
	}

}
