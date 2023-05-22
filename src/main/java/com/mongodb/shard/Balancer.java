package com.mongodb.shard;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import org.bson.Document;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.shardsync.ShardClient;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "balancer", mixinStandardHelpOptions = true, version = "balancer 0.1", description = "Custom mongodb shard balancer")
public class Balancer implements Callable<Integer> {

	private static Logger logger = LoggerFactory.getLogger(Balancer.class);

	@Option(names = { "--uri" }, description = "mongodb uri connection string", required = true)
	private String uri;

	private MongoClient mongoClient;

	@Override
	public Integer call() throws Exception {

		ShardClient shardClient = new ShardClient("source", uri);
		shardClient.init();
		shardClient.stopBalancer();
		mongoClient = shardClient.getMongoClient();

		MongoDatabase db = mongoClient.getDatabase("config");
		MongoCollection<Document> chunks = db.getCollection("chunks");

		MongoCursor<Document> chunkCountIt = chunks
				.aggregate(Arrays.asList(match(Filters.ne("ns", "config.system.sessions")),
						group("$ns", Accumulators.sum("count", 1)), sort(Sorts.descending("count"))))
				.iterator();

		int totalMoved = 0;
		
		while (chunkCountIt.hasNext()) {
			Document chunksCount = chunkCountIt.next();

			logger.debug("chunk aggregate: {}", chunksCount);

			String ns = chunksCount.getString("_id");

			List<Document> chunksCountResults = new ArrayList<>();
			chunks.aggregate(Arrays.asList(match(Filters.eq("ns", ns)), group("$shard", Accumulators.sum("count", 1)),
					sort(Sorts.descending("count")))).into(chunksCountResults);

			if (chunksCountResults.size() >= 2) {

				Document highestCount = chunksCountResults.get(0);
				Document lowestCount = chunksCountResults.get(chunksCountResults.size() - 1);
				
				String sourceShard = highestCount.getString("_id");
				String destShard = lowestCount.getString("_id");
				
				logger.debug("{} - preparing to move ({}->{}), highestCount: {}, lowestCount: {}", ns, sourceShard, destShard, highestCount, lowestCount);
				
				List<RawBsonDocument> sourceChunks = new ArrayList<>();
				chunks.find(Filters.and(Filters.eq("ns", ns), Filters.eq("shard", sourceShard)), RawBsonDocument.class)
						.into(sourceChunks);
				Collections.shuffle(sourceChunks);
				
				int sourceCount = highestCount.getInteger("count");
				int destCount = lowestCount.getInteger("count");
				int totalCount = sourceCount + destCount;
				int targetChunks = totalCount / 2;
				int chunksToMove = sourceCount - targetChunks;
				
				logger.debug("{} - preparing to move ({}->{}), highestCount: {}, lowestCount: {}", ns, sourceShard, destShard, highestCount, lowestCount);
				logger.debug("chunksToMove: {}, targetCount: {}", chunksToMove, targetChunks);

				for (int i = 0; i < chunksToMove; i++) {
					RawBsonDocument chunk = sourceChunks.get(i);
					shardClient.moveChunk(chunk, destShard, false, true, true);
					totalMoved++;
					logger.debug("Moved chunk: {}, totalMoved: {}", chunk.get("_id"), totalMoved);
				}
			}

		}

		return 0;
	}

	

	public static void main(String... args) {
		int exitCode = new CommandLine(new Balancer()).execute(args);
		System.exit(exitCode);
	}

}
