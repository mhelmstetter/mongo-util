package com.mongodb.diff3;

import java.util.LinkedList;
import java.util.List;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

public class ShardedDiffResult extends DiffResult {
	private RawBsonDocument chunk;
	private Bson chunkQuery;
	private String ns;

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
		builder.append(failedIds);
		builder.append(", chunkQuery");
		builder.append(chunkQuery);
		builder.append("]");
		return builder.toString();
	}

	public String shortString() {
		StringBuilder sb = new StringBuilder();
		sb.append("ns=");
		sb.append(ns);
		sb.append(", query=");
		sb.append(chunkQuery);
		return sb.toString();
	}

	public Bson getChunkQuery() {
		return chunkQuery;
	}

	public void setChunkQuery(Bson chunkQuery) {
		this.chunkQuery = chunkQuery;
	}

	public void setNS(String ns){
		this.ns = ns;
	}

	public String getNs() {
		return ns;
	}

}
