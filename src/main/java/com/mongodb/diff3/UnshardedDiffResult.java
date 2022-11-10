package com.mongodb.diff3;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

import java.util.LinkedList;
import java.util.List;

public class UnshardedDiffResult extends DiffResult {

	public UnshardedDiffResult(String ns) {
		this.ns = ns;
	}
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DiffResult [ns=");
		builder.append(ns);
		builder.append(", matches=");
		builder.append(matches);
		builder.append(", failedIds=");
		builder.append(failedIds.size());
		builder.append("]");
		return builder.toString();
	}
}
