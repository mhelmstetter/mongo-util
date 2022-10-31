package com.mongodb.diff3;

import java.util.LinkedList;
import java.util.List;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;

public class DiffResult {
	
	long matches = 0;
	
	List<BsonValue> failedIds;
	
	private RawBsonDocument chunk;
	
	
	public void addFailedKey(BsonValue id) {
		if (failedIds == null) {
			failedIds = new LinkedList<BsonValue>();
		}
		failedIds.add(id);
		
	}
	
	public int getFailureCount() {
		return (failedIds == null) ? 0 : failedIds.size();
	}



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
		builder.append("matches=");
		builder.append(matches);
		builder.append(", failedIds=");
		builder.append(failedIds);
		builder.append("]");
		return builder.toString();
	}
}
