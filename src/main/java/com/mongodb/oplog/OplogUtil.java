package com.mongodb.oplog;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.bson.RawBsonDocument;

import com.mongodb.model.Namespace;
import com.mongodb.model.OplogOpType;
import com.mongodb.model.OplogSummary;

public class OplogUtil {

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

	public static Object getIdFromInsertOrDeleteOplogEntry(RawBsonDocument oplogEntry) {
		RawBsonDocument o = (RawBsonDocument) oplogEntry.get("o");
		return o.get("_id");
	}

	public static Object getIdFromUpdateOplogEntry(RawBsonDocument oplogEntry) {
		RawBsonDocument o2 = (RawBsonDocument) oplogEntry.get("o2");
		return o2.get("_id");
	}

}
