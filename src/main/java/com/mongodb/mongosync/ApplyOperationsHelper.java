package com.mongodb.mongosync;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;
import com.mongodb.shardsync.ShardClient;

public class ApplyOperationsHelper  {
	
	protected static final Logger logger = LoggerFactory.getLogger(ApplyOperationsHelper.class);
	
	private OplogTailMonitor oplogTailMonitor;
	private ShardClient destShardClient;
	private String shardId;
	private final static BulkWriteOptions orderedBulkWriteOptions = new BulkWriteOptions();
	private final static BulkWriteOptions unorderedBulkWriteOptions = new BulkWriteOptions().ordered(false);
	
	
	public ApplyOperationsHelper(String shardId, OplogTailMonitor oplogTailMonitor, ShardClient destShardClient) {
		this.shardId = shardId;
		this.oplogTailMonitor = oplogTailMonitor;
		this.destShardClient = destShardClient;
	}
	
	
	private void applyBulkWriteModelsOnCollection(Namespace namespace, OplogBatch batch, List<WriteModel<BsonDocument>> writeModels,
			boolean useOrdered,
			final BulkWriteOutput output)
			throws MongoException {
		MongoCollection<BsonDocument> collection = destShardClient.getCollectionRaw(namespace);
		BulkWriteResult bulkWriteResult = null;
		
		try {
			//BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(collection, operations, originalOps);
			if (useOrdered) {
				//logger.debug("{}: using ordered batch, {} ops", shardId, writeModels.size());
				bulkWriteResult = collection.bulkWrite(writeModels, orderedBulkWriteOptions);
			} else {
				//logger.debug("{}: using un-ordered batch, {} ops", shardId, writeModels.size());
				bulkWriteResult = collection.bulkWrite(writeModels, unorderedBulkWriteOptions);
			}
			
			output.increment(bulkWriteResult);
		} catch (MongoBulkWriteException err) {
			List<BulkWriteError> errors = err.getWriteErrors();
			bulkWriteResult = err.getWriteResult();
			output.increment(bulkWriteResult, errors.size());
			//output = new BulkWriteOutput(bulkWriteResult);
			if (errors.size() == writeModels.size()) {
				//logger.error("{} bulk write errors, all {} operations failed: {}", shardId, err);
			} else {
				if (useOrdered) {
					for (BulkWriteError bwe : errors) {
						int index = bwe.getIndex();
						//operations.remove(index);
						batch.removeWriteModel(writeModels.get(index), true);
					}
					//logger.debug("{} retrying {} operations, errorCount: {}", shardId, batch.size(), errors.size());
					//logger.warn("{}: bulk write errors", shardId, err);
					batch.clearUnordered();
					applyBulkWriteModelsOnCollection(namespace, batch, output);
					//return applySoloBulkWriteModelsOnCollection(operations, collection, output);
				}
			}
		} catch (Exception ex) {
			logger.error("{} unknown error: {}", shardId, ex.getMessage(), ex);
		}
	}
	
	public BulkWriteOutput applyBulkWriteModelsOnCollection(Namespace namespace, OplogBatch oplogBatch) {
		BulkWriteOutput output = new BulkWriteOutput();
		applyBulkWriteModelsOnCollection(namespace, oplogBatch, output);
		return output;
	}
	
	private void applyBulkWriteModelsOnCollection(Namespace namespace, OplogBatch oplogBatch, final BulkWriteOutput output)
			throws MongoException {
		
		List<WriteModel<BsonDocument>> orderedWriteModels = oplogBatch.getOrderedWriteModels();
		if (!orderedWriteModels.isEmpty()) {
			applyBulkWriteModelsOnCollection(namespace, oplogBatch, orderedWriteModels, true, output);
		}
		List<WriteModel<BsonDocument>> unorderedWriteModels = oplogBatch.getUnorderedWriteModels();
		if (!unorderedWriteModels.isEmpty()) {
			applyBulkWriteModelsOnCollection(namespace, oplogBatch, unorderedWriteModels, false, output);
		}
	}
	
	private BulkWriteOutput applySoloBulkWriteModelsOnCollection(List<WriteModel<BsonDocument>> operations, 
			MongoCollection<BsonDocument> collection, BulkWriteOutput output) {
        BulkWriteResult soloResult = null;
        int i = 1;
        for (WriteModel<BsonDocument> op : operations) {
            List<WriteModel<BsonDocument>> soloBulkOp = new ArrayList<>();
            soloBulkOp.add(op);
            try {
            	soloResult = collection.bulkWrite(operations, orderedBulkWriteOptions);
            	logger.debug("{}: retry {} of {} - {}", shardId, i, operations.size(), output);
            	output.incDeleted(soloResult.getDeletedCount());
            	output.incInserted(soloResult.getInsertedCount());
            	output.incModified(soloResult.getModifiedCount());
            	output.incUpserted(soloResult.getUpserts().size());
            } catch (MongoBulkWriteException bwe) {
                output.incDuplicateKeyExceptionCount(1);
            } catch (Exception soloErr) {
            	output.incDuplicateKeyExceptionCount(1);
                logger.error("[BULK-WRITE-RETRY] unknown exception occurred while applying solo op {} on collection: {}; error {}",
                        op.toString(), collection.getNamespace().getFullName(), soloErr.toString());
            }
            i++;
        }
        return output;
    }
	
	public static WriteModel<BsonDocument> getWriteModelForOperation(BsonDocument operation) throws MongoException {
		String message;
		WriteModel<BsonDocument> model = null;
		switch (operation.getString("op").getValue()) {
		case "i":
			model = getInsertWriteModel(operation);
			break;
		case "u":
			model = getUpdateWriteModel(operation);
			break;
		case "d":
			model = getDeleteWriteModel(operation);
			break;
		case "n":
			break;
		default:
			message = String.format("unsupported operation %s; op: %s", operation.getString("op"), operation.toJson());
			logger.error(message);
		}
		return model;
	}
	
	private void performRunCommand(BsonDocument operation) {
		try {
			BsonDocument document = operation.getDocument("o");
			//String databaseName = operation.getString("ns").getValue().replace(".$cmd", "");

			//logger.debug("performRunCommand: {}", databaseName);
			logger.debug("{}: performRunCommand, op: {}", shardId, operation);
			MongoDatabase database = destShardClient.getMongoClient().getDatabase("admin");
			database.runCommand(document);
			logger.debug("{}: completed performRunCommand, op: {}", shardId, operation);
		} catch (MongoException me) {
			logger.debug("{}: performRunCommand error, op: {}, error: {}", shardId, operation, me.getMessage());
		}
		
	}
	
	private static WriteModel<BsonDocument> getInsertWriteModel(BsonDocument operation) {
		String ns = operation.getString("ns").getValue();
		BsonDocument document = operation.getDocument("o");
		// TODO can we get duplicate key here?
		return new InsertOneModel<>(document);
	}

	private static WriteModel<BsonDocument> getUpdateWriteModel(BsonDocument operation) {
		
		BsonDocument find = operation.getDocument("o2");
		BsonDocument update = operation.getDocument("o");

		if (update.containsKey("$v")) {
			update.remove("$v");
		}

		// if the update operation is not using $set/$push, etc then use replaceOne
		Set<String> docKeys = update.keySet();
		if (docKeys.iterator().next().startsWith("$")) {
			return new UpdateOneModel<BsonDocument>(find, update);
		} else {
			return new ReplaceOneModel<BsonDocument>(find, update);
		}
	}

	private static WriteModel<BsonDocument> getDeleteWriteModel(BsonDocument operation) throws MongoException {
		BsonDocument find = operation.getDocument("o");
		return new DeleteOneModel<>(find);
	}

}
