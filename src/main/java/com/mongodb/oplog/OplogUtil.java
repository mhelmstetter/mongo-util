package com.mongodb.oplog;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.mongodb.model.Namespace;
import com.mongodb.model.OplogOpType;
import com.mongodb.model.OplogSummary;
import com.mongodb.shardbalancer.TailingOplogAnalyzerWorker;
import com.mongodb.util.bson.BsonValueWrapper;

public class OplogUtil {
	
	protected static final Logger logger = LoggerFactory.getLogger(OplogUtil.class);

	private final static Set<String> databasesBlacklist = new HashSet<>(
			Arrays.asList("system", "local", "config", "admin"));

	public static OplogSummary getOplogSummaryFromOplogEntry(RawBsonDocument oplogEntry) {

		String opTypeCode = oplogEntry.getString("op").getValue();
		OplogOpType opType = OplogOpType.fromCode(opTypeCode);

		String nsStr = oplogEntry.getString("ns").getValue();
		Namespace ns = new Namespace(nsStr);
		if (databasesBlacklist.contains(ns.getDatabaseName())) {
			return new OplogSummary(ns, null, opType);
		}
		Object id = null;

		switch (opType) {
		case INSERT:
		case DELETE:
			id = getIdFromInsertOrDeleteOplogEntry(oplogEntry);
			return new OplogSummary(ns, id, opType);
		case UPDATE:
			id = getIdFromUpdateOplogEntry(oplogEntry);
			return new OplogSummary(ns, id, opType);
		default:
			return new OplogSummary(ns, null, opType);

		}

	}
	
	public static BsonValueWrapper getIdForOperation(BsonDocument operation, Set<String> shardKey, String shardId) throws MongoException {
		String opType = operation.getString("op").getValue();
		switch (opType) {
		case "u":
			BsonDocument o2 = operation.getDocument("o2");
			if (o2 != null) {
				if (shardKey.size() == 1) {
					String key = shardKey.iterator().next();
					BsonValue id = o2.get(key);
					if (id != null) {
						return new BsonValueWrapper(id);
					} else {
						logger.warn("{}: did not find o2._id field for update oplog entry: {}", shardId, operation);
					}
				} else if (shardKey.size() == o2.size()) {
					return new BsonValueWrapper(o2);
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
					return new BsonValueWrapper(id);
				} else {
					logger.warn("{}: did not find o._id field for insert/delete oplog entry: {}", shardId, operation);
				}
			} else {
				logger.error("{}: did not find o field for insert/delete oplog entry: {}", shardId, operation);
			}
			break;
		}
		return null;
	}

	public static Object getIdFromInsertOrDeleteOplogEntry(RawBsonDocument oplogEntry) {
		RawBsonDocument o = (RawBsonDocument) oplogEntry.get("o");
		return o.get("_id");
	}

	public static Object getIdFromUpdateOplogEntry(RawBsonDocument oplogEntry) {
		RawBsonDocument o2 = (RawBsonDocument) oplogEntry.get("o2");
		return o2.get("_id");
	}

}
