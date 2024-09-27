package com.mongodb.oplog;

import java.util.Set;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

public class ApplyOperationsHelper  {
	
	protected static final Logger logger = LoggerFactory.getLogger(ApplyOperationsHelper.class);
	
	private final static UpdateOptions upsertOptions = new UpdateOptions().upsert(true);
	
	
	public static WriteModel<BsonDocument> getWriteModelForOperation(BsonDocument operation) throws MongoException {
		return getWriteModelForOperation(operation, false);
	}
	
	public static WriteModel<BsonDocument> getWriteModelForOperation(BsonDocument operation, boolean upsert) throws MongoException {
		String message;
		WriteModel<BsonDocument> model = null;
		switch (operation.getString("op").getValue()) {
		case "i":
			model = getInsertWriteModel(operation);
			break;
		case "u":
			model = getUpdateWriteModel(operation, upsert);
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
	
	private static WriteModel<BsonDocument> getInsertWriteModel(BsonDocument operation) {
		String ns = operation.getString("ns").getValue();
		BsonDocument document = operation.getDocument("o");
		// TODO can we get duplicate key here?
		return new InsertOneModel<>(document);
	}

	private static WriteModel<BsonDocument> getUpdateWriteModel(BsonDocument operation, boolean upsert) {
		
		BsonDocument find = operation.getDocument("o2");
		BsonDocument update = operation.getDocument("o");

		if (update.containsKey("$v")) {
			update.remove("$v");
		}

		// if the update operation is not using $set/$push, etc then use replaceOne
		Set<String> docKeys = update.keySet();
		if (docKeys.iterator().next().startsWith("$")) {
			if (upsert) {
				return new UpdateOneModel<BsonDocument>(find, update, upsertOptions);
			} else {
				return new UpdateOneModel<BsonDocument>(find, update);
			}
			
		} else {
			return new ReplaceOneModel<BsonDocument>(find, update);
		}
	}

	private static WriteModel<BsonDocument> getDeleteWriteModel(BsonDocument operation) throws MongoException {
		BsonDocument find = operation.getDocument("o");
		return new DeleteOneModel<>(find);
	}

}
