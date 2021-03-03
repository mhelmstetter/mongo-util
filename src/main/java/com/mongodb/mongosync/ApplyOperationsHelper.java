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
	private final static BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
	
	
	public ApplyOperationsHelper(String shardId, OplogTailMonitor oplogTailMonitor, ShardClient destShardClient) {
		this.shardId = shardId;
		this.oplogTailMonitor = oplogTailMonitor;
		this.destShardClient = destShardClient;
	}
	
	

	public void applyOperations(List<BsonDocument> operations) throws Exception {
		
		List<WriteModel<BsonDocument>> models = new Vector<>();
		

		Namespace namespace = null;
		BsonTimestamp lastTimestamp = null;
		
		
		for (BsonDocument currentDocument : operations) {
			String ns = currentDocument.getString("ns").getValue();
			if (namespace == null) {
				namespace = new Namespace(ns);
			}
			String op = currentDocument.getString("op").getValue();
			if (op.equals("c")) {
				continue;
				//performRunCommand(currentDocument);
			} 
			
			WriteModel<BsonDocument> model = getWriteModelForOperation(currentDocument);
			if (model != null) {
				models.add(model);
			} else {
				// if the command is $cmd for create index or create collection, there would not
				// be any write model.
				logger.warn(String.format(
						"ignoring oplog entry. could not convert the document to model. Given document is [%s]",
						currentDocument.toJson()));
			}
			lastTimestamp = currentDocument.getTimestamp("ts");
		}

		if (models.size() > 0) {
			BulkWriteOutput output = applyBulkWriteModelsOnCollection(namespace, models);
			oplogTailMonitor.updateStatus(output);
			oplogTailMonitor.setLatestTimestamp(lastTimestamp);
		}
		operations = null;
		models = null;
	}
	
	
	public BulkWriteOutput applyBulkWriteModelsOnCollection(Namespace namespace, List<WriteModel<BsonDocument>> operations)
			throws MongoException {
		MongoCollection<BsonDocument> collection = destShardClient.getCollectionRaw(namespace);
		BulkWriteResult bulkWriteResult = null;
		BulkWriteOutput output = null;
		try {
			//BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(collection, operations, originalOps);
			bulkWriteResult = collection.bulkWrite(operations, bulkWriteOptions);
			output = new BulkWriteOutput(bulkWriteResult);
			return output;
		} catch (MongoBulkWriteException err) {
			List<BulkWriteError> errors = err.getWriteErrors();
			bulkWriteResult = err.getWriteResult();
			output = new BulkWriteOutput(bulkWriteResult);
			if (errors.size() == operations.size()) {
				logger.error("{} bulk write errors, all {} operations failed: {}", shardId, err);
				return output;
			} else {
				for (BulkWriteError bwe : errors) {
					int index = bwe.getIndex();
					operations.remove(index);
				}
				//logger.debug("{} retrying {} operations", shardId, operations.size());
				return applySoloBulkWriteModelsOnCollection(operations, collection, output);
			}
		} catch (Exception ex) {
			logger.error("{} unknown error: {}", shardId, ex.getMessage(), ex);
		}
		return new BulkWriteOutput(0, 0, 0, 0, operations.size());
	}
	
	private BulkWriteOutput applySoloBulkWriteModelsOnCollection(List<WriteModel<BsonDocument>> operations, 
			MongoCollection<BsonDocument> collection, BulkWriteOutput output) {
        BulkWriteResult soloResult = null;
        for (WriteModel<BsonDocument> op : operations) {
            List<WriteModel<BsonDocument>> soloBulkOp = new ArrayList<>();
            soloBulkOp.add(op);
            try {
            	soloResult = collection.bulkWrite(operations, bulkWriteOptions);
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

		// if the update operation is not using $set then use replaceOne
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
