package com.mongodb.mongosync;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.ne;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoWriteException;
import com.mongodb.bulk.BulkWriteError;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.WriteModel;
import com.mongodb.model.Namespace;
import com.mongodb.model.ShardTimestamp;
import com.mongodb.shardsync.ShardClient;

public class OplogTailWorker implements Runnable {

	protected static final Logger logger = LoggerFactory.getLogger(OplogTailWorker.class);

	private String shardId;
	private ShardTimestamp shardTimestamp;

	private ShardClient sourceShardClient;
	private ShardClient destShardClient;
	private MongoSyncOptions options;
	
	//private TimestampFile timestampFile;
	
	private ClientSession destSession;
	
	private OplogTailMonitor oplogTailMonitor;

	public OplogTailWorker(ShardTimestamp shardTimestamp, TimestampFile timestampFile, ShardClient sourceShardClient, ShardClient destShardClient,
			MongoSyncOptions options) throws IOException {
		this.shardId = shardTimestamp.getShardName();
		this.shardTimestamp = shardTimestamp;
		this.sourceShardClient = sourceShardClient;
		this.destShardClient = destShardClient;
		this.destSession = destShardClient.getMongoClient().startSession();
		this.options = options;
		oplogTailMonitor = new OplogTailMonitor(timestampFile, sourceShardClient);
	}

	@Override
	public void run() {
		
        ScheduledExecutorService monitorExecutor = Executors.newScheduledThreadPool(1);
        monitorExecutor.scheduleAtFixedRate(oplogTailMonitor, 0L, 30L, TimeUnit.SECONDS);
		
		MongoDatabase local = sourceShardClient.getShardMongoClient(shardId).getDatabase("local");
		MongoCollection<BsonDocument> oplog = local.getCollection("oplog.rs", BsonDocument.class);

		Set<String> namespacesToMigrate = options.getNamespacesToMigrate();

		List<BsonDocument> buffer = new ArrayList<BsonDocument>(options.getBatchSize());

		MongoCursor<BsonDocument> cursor = null;
		// TODO - should we ignore no-ops
		// appears that mongomirror does not for keeping the timestamp up to date
		//Bson query = and(gte("ts", shardTimestamp.getTimestamp()), ne("op", "n")); 
		Bson query = gte("ts", shardTimestamp.getTimestamp()); 
		
		long start = System.currentTimeMillis();
		long count;
		try {
			
			logger.debug("{}: starting oplog tail query", shardId);
			count = 0;
			cursor = oplog.find(query).sort(eq("$natural", 1))
					.oplogReplay(true)
					.noCursorTimeout(true)
					.cursorType(CursorType.TailableAwait).iterator();
			while (cursor.hasNext()) {
				BsonDocument doc = cursor.next();
				String ns = doc.getString("ns").getValue();

				if (namespacesToMigrate.contains(ns)) {
					//logger.debug(shardId + " " + doc);
					count++;
				}

                buffer.add(doc);
                if (buffer.size() >= options.getBatchSize()) {
                	applyOperations(buffer);
                    buffer.clear();
                }

			}
			
            // flush any remaining from the buffer
            if (buffer.size() > 0) {
            	applyOperations(buffer);
            }
			
			long end = System.currentTimeMillis();
			Double dur = (end - start) / 1000.0;
			logger.debug(String.format("\nDone cloning, %s documents in %f seconds",  count, dur));
		
		} catch (Exception e) {
			logger.error("tail error", e);
		} finally {
			if (buffer.size() > 0) {
            	try {
					applyOperations(buffer);
				} catch (MongoException | IOException e) {
					logger.error("final apply error", e);
				}
            }
			cursor.close();
		}
		
	}

	public int applyOperations(List<BsonDocument> operations) throws MongoException, IOException {
		int totalModelsAdded = 0;
		int totalValidOperations = 0;
		Namespace previousNamespace = null;
		BsonDocument previousDocument = null;
		List<WriteModel<BsonDocument>> models = new ArrayList<>();

		for (int i = 0; i < operations.size(); i++) {
			BsonDocument currentDocument = operations.get(i);
			String ns = currentDocument.getString("ns").getValue();
			if (ns == null || ns.equals("")) {
				continue;
			}
			Namespace currentNamespace = new Namespace(ns);
			
			if (currentNamespace.hasDatabase("config")) {
				continue;
			}

			// modify namespace via namespacesRename
			// currentNamespace =
			// modificationHelper.getMappedNamespace(currentDocument.getString("ns"));

			if (!currentNamespace.equals(previousNamespace)) {
				// change of namespace. bulk apply models for previous namespace
				if (previousNamespace != null && models.size() > 0) {
					BulkWriteOutput output = applyBulkWriteModelsOnCollection(previousNamespace, models);
					if (operations.size() == output.getSuccessfulWritesCount()) {
						if (logger.isTraceEnabled()) {
							logger.trace("all the {} write operations for the {} batch were applied successfully",
									operations.size(), currentNamespace);
						}
					} else {
						// TODO this is noisy and figure out why the counts don't match
						logger.warn("applyOperations the {} write operations for the batch were applied fully; output {}",
								operations.size(), output.toString());
					}
					totalModelsAdded += output.getSuccessfulWritesCount();
					models.clear();
					oplogTailMonitor.updateStatus(output);
					oplogTailMonitor.setLatestTimestamp(previousDocument);
				}
				previousNamespace = currentNamespace;
				previousDocument = currentDocument;
			}
			WriteModel<BsonDocument> model = getWriteModelForOperation(currentDocument);
			if (model != null) {
				models.add(model);
				totalValidOperations++;
			} else {
				// if the command is $cmd for create index or create collection, there would not
				// be any write model.
				logger.warn(String.format(
						"ignoring oplog entry. could not convert the document to model. Given document is [%s]",
						currentDocument.toJson()));
			}
		}

		if (models.size() > 0) {
			BulkWriteOutput output = applyBulkWriteModelsOnCollection(previousNamespace, models);
			if (output != null) {
				oplogTailMonitor.updateStatus(output);
				totalModelsAdded += output.getSuccessfulWritesCount(); // bulkWriteResult.getUpserts().size();
				oplogTailMonitor.setLatestTimestamp(previousDocument);
			}
		}

		// TODO: What happens if there is duplicate exception?
//		if (totalModelsAdded != totalValidOperations) {
//			logger.warn(
//					"[FATAL] total models added {} is not equal to operations injected {}. grep the logs for BULK-WRITE-RETRY",
//					totalModelsAdded, operations.size());
//		}

		return totalModelsAdded;
	}
	
	private WriteModel<BsonDocument> getWriteModelForOperation(BsonDocument operation) throws MongoException {
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
		case "c":
			// might have to be individual operation
			performRunCommand(operation);
			break;
		case "n":
			break;
		default:
			message = String.format("unsupported operation %s; op: %s", operation.getString("op"), operation.toJson());
			logger.error(message);
			throw new MongoException(message);
		}
		return model;
	}
	
	private BulkWriteResult applyBulkWriteModelsOnCollection(MongoCollection<BsonDocument> collection, 
			List<WriteModel<BsonDocument>> operations) throws MongoException {
        BulkWriteResult writeResult = performOperationWithRetry(
                () -> {
                    BulkWriteOptions options = new BulkWriteOptions();
                    options.ordered(true);
                    return collection.bulkWrite(destSession, operations, options);
                }
                , new BsonDocument("operation", new BsonString("bulkWrite")));
        return writeResult;
    }

	private BulkWriteOutput applyBulkWriteModelsOnCollection(Namespace namespace, List<WriteModel<BsonDocument>> operations)
			throws MongoException {
		MongoCollection<BsonDocument> collection = destShardClient.getCollectionRaw(namespace);
		try {
			BulkWriteResult bulkWriteResult = applyBulkWriteModelsOnCollection(collection, operations);
			BulkWriteOutput output = new BulkWriteOutput(bulkWriteResult);
			return output;
		} catch (MongoBulkWriteException err) {
			if (err.getWriteErrors().size() == operations.size()) {
				// every doc in this batch is error. just move on
				logger.debug(
						"[IGNORE] Ignoring all the {} write operations for the {} batch as they all failed with duplicate key exception. (already applied previously)",
						operations.size(), namespace);
				return new BulkWriteOutput(0, 0, 0, 0, operations.size(), new ArrayList<>());
			}
			logger.warn(
					"[WARN] the {} bulk write operations for the {} batch failed with exceptions. applying them one by one. error: {}",
					operations.size(), namespace, err.getWriteErrors().toString());
			return applySoloBulkWriteModelsOnCollection(operations, collection);
		} catch (Exception ex) {
			logger.error(
					"[FATAL] unknown exception occurred while apply bulk write options for oplog. err: {}. Going to retry individually anyways",
					ex.toString());
			return applySoloBulkWriteModelsOnCollection(operations, collection);
		}
	}
	
    private BulkWriteOutput applySoloBulkWriteModelsOnCollection(List<WriteModel<BsonDocument>> operations, MongoCollection<BsonDocument> collection) {
        BulkWriteResult soloResult = null;
        List<WriteModel<BsonDocument>> failedOps = new ArrayList<>();
        int deletedCount = 0;
        int modifiedCount = 0;
        int insertedCount = 0;
        int upsertedCount = 0;
        for (WriteModel<BsonDocument> op : operations) {
            List<WriteModel<BsonDocument>> soloBulkOp = new ArrayList<>();
            soloBulkOp.add(op);
            try {
                soloResult = applyBulkWriteModelsOnCollection(collection, soloBulkOp);
                deletedCount += soloResult.getDeletedCount();
                modifiedCount += soloResult.getModifiedCount();
                insertedCount += soloResult.getInsertedCount();
                upsertedCount += soloResult.getUpserts().size();
              
//                logger.info("[BULK-WRITE-RETRY SUCCESS] retried solo op {} on collection: {} produced result: {}",
//                        op.toString(), collection.getNamespace().getFullName(), soloResult.toString());
                // no errors? keep going
            } catch (MongoBulkWriteException bwe) {
                BulkWriteError we = bwe.getWriteErrors().get(0);
                if (bwe.getCode() != 11000) {
                	failedOps.add(op);
                    logger.error("[BULK-WRITE-RETRY ERROR] solo op on collection: {}; error {}",
                            collection.getNamespace().getFullName(), bwe.getMessage());
                }
            } catch (Exception soloErr) {
                failedOps.add(op);
                logger.error("[BULK-WRITE-RETRY] unknown exception occurred while applying solo op on collection: {}; error {}",
                        collection.getNamespace().getFullName(), soloErr.toString());
            }
        }
        BulkWriteOutput output = new BulkWriteOutput(deletedCount, modifiedCount, insertedCount, upsertedCount, 0, failedOps); //TODO: upsertedCount ??
        logger.info("[BULK-WRITE-RETRY] all the {} operations for the batch {} were retried one-by-one. result {}",
                operations.size(), collection.getNamespace().getFullName(), output.toString());
        return output;
    }

	private WriteModel<BsonDocument> getInsertWriteModel(BsonDocument operation) {
		BsonDocument document = operation.getDocument("o");
		// TODO can we get duplicate key here?
		return new InsertOneModel<>(document);
	}

	private WriteModel<BsonDocument> getUpdateWriteModel(BsonDocument operation) {
		
		BsonDocument find = operation.getDocument("o2");
		BsonDocument update = operation.getDocument("o");

		if (update.containsKey("$v")) {
			update.remove("$v");
		}

		// if the update operation is not using $set then use replaceOne
		Set<String> docKeys = update.keySet();
		if (docKeys.size() == 1 && docKeys.iterator().next().startsWith("$")) {
			return new UpdateOneModel<BsonDocument>(find, update);
		} else {
			return new ReplaceOneModel<BsonDocument>(find, update);
		}
	}

	private WriteModel<BsonDocument> getDeleteWriteModel(BsonDocument operation) throws MongoException {
		BsonDocument find = operation.getDocument("o");
		return new DeleteOneModel<>(find);
	}

	private void performRunCommand(BsonDocument origOperation) throws MongoException {
		BsonDocument operation = getMappedOperation(origOperation);
		BsonDocument document = operation.getDocument("o");
		String databaseName = operation.getString("ns").getValue().replace(".$cmd", "");

		logger.debug("performRunCommand: {}", databaseName);
		logger.debug("performRunCommand, modified operation: {}", operation);
		MongoDatabase database = destShardClient.getMongoClient().getDatabase(databaseName);
		performOperationWithRetry(() -> {
			database.runCommand(document);
			return 1L;
		}, operation);

		String message = String.format("completed runCommand op on database: %s; document: %s", databaseName,
				operation.toJson());
		logger.debug(message);
	}

	private BsonDocument getMappedOperation(BsonDocument operation) {
		BsonDocument document = operation.getDocument("o");
		if (!document.containsKey("create") && !document.containsKey("drop") && !document.containsKey("create")) {
			return operation;
		}
		updateOperationWithMappedCollectionIfRequired(operation, document, "drop");
		updateOperationWithMappedCollectionIfRequired(operation, document, "create");
		return operation;
	}

	private void updateOperationWithMappedCollectionIfRequired(BsonDocument operation, BsonDocument document,
			String operationName) {
		if (document.containsKey(operationName)) {
			String databaseName = operation.getString("ns").getValue().replace(".$cmd", "");
			String collectionName = document.getString(operationName).getValue();

			// Resource mappedResource = modificationHelper.getMappedResource(new
			// Resource(databaseName, collectionName));
			document.put(operationName, new BsonString(collectionName));
			// operation.put("ns", mappedResource.getDatabase() + ".$cmd");
			operation.put("ns", new BsonString(databaseName + ".$cmd"));

			if (operationName == "create") {
				updateIdIndexOperationWithMappedNamespace(document,
						String.format("%s.%s", databaseName, collectionName));
			}
		}
	}

	private void updateIdIndexOperationWithMappedNamespace(BsonDocument document, String namespace) {
		if (document.containsKey("idIndex")) {
			BsonDocument index = document.getDocument("idIndex");
			index.put("ns", new BsonString(namespace));
		}
	}

	public <T> T performOperationWithRetry(Supplier<T> operationFunc, BsonDocument operation) throws MongoException {
		for (int retry = 0; retry <= 1; retry++) {
			try {
				return operationFunc.get();
			} catch (MongoNotPrimaryException | MongoSocketException | MongoNodeIsRecoveringException me) {
				// do a retry
				logger.error("[RETRY] Failed to perform operation. Will retry once; error: {}", me.toString());
			} catch (MongoBulkWriteException bwe) {
				if (bwe.getMessage().contains("E11000 duplicate key error collection")) {
					//logger.debug("[IGNORE]  Duplicate key exception while performing operation: {}; error: {}", operation.toJson(), bwe.toString());
					throw bwe;
				}
				logger.error("performOperationWithRetry error: {}", bwe.toString());
				throw bwe;
			} catch (MongoWriteException we) {
				if (we.getMessage().startsWith("E11000 duplicate key error collection")) {
					//logger.debug("[IGNORE]  Duplicate key exception: {}", we.toString());
					return null;
				}
				logger.error("performOperationWithRetry op error: {}", we.toString());
				throw we;
			} catch (Exception e) {
				logger.error("performOperationWithRetry [UNHANDLED] error: {}", e.toString());
				throw e;
			}
		}
		return null;
	}
	
	

}
