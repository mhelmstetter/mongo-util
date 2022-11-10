package com.mongodb.diff3;

import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

public class ShardedDiffResult extends DiffResult {
	private RawBsonDocument chunk;
	private Bson chunkQuery;

	public RawBsonDocument getChunk() {
		return chunk;
	}

	public void setChunk(RawBsonDocument chunk) {
		this.chunk = chunk;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("DiffResult [ns=");
		builder.append(chunk.getString("ns").getValue());
		builder.append(", matches=");
		builder.append(matches);
		builder.append(", failedIds=");
		builder.append(failedIds.size());
		builder.append(", chunk=");
		builder.append(chunkString);
		builder.append("]");
		return builder.toString();
	}

	public String shortString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ns=");
		sb.append(ns);
		sb.append(", chunk=");
		sb.append(chunkString);
		return sb.toString();
	}

	public Bson getChunkQuery() {
		return chunkQuery;
	}

	public void setChunkQuery(Bson chunkQuery) {
		this.chunkQuery = chunkQuery;
	}

}
