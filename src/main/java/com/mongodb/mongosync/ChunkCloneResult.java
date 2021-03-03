package com.mongodb.mongosync;

import com.mongodb.model.Namespace;

public class ChunkCloneResult {
	
	public long sourceCount = 0;
	
	public long successCount;
	public long errorCount;
	public long duplicateKeyCount;
	private Namespace ns;
	
	public ChunkCloneResult(Namespace ns) {
		this.ns = ns;
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
		builder.append("]");
		return builder.toString();
	}

}
