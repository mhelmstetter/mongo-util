package com.mongodb.mongosync;

import org.bson.conversions.Bson;

import com.mongodb.model.Namespace;

public class ChunkCloneResult {
	
	public long sourceCount = 0;
	
	public long successCount;
	public long errorCount;
	public long duplicateKeyCount;
	public long skippedCount;
	private Namespace ns;
	private Bson chunkQuery;
	
	public ChunkCloneResult(Namespace ns, Bson chunkQuery) {
		this.ns = ns;
		this.chunkQuery = chunkQuery;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ChunkCloneResult [ns=");
		builder.append(ns);
		builder.append(", sourceCount=");
		builder.append(sourceCount);
		builder.append(", successCount=");
		builder.append(successCount);
		builder.append(", errorCount=");
		builder.append(errorCount);
		builder.append(", duplicateKeyCount=");
		builder.append(duplicateKeyCount);
		builder.append(", skippedCount=");
		builder.append(skippedCount);
//		builder.append(", chunkQuery=");
//		builder.append(chunkQuery);
		builder.append("]");
		return builder.toString();
	}

}
