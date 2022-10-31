package com.mongodb.diff3;

import java.util.LinkedList;
import java.util.List;

import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.conversions.Bson;

public class DiffResult {
	
	long matches = 0;
	long onlyOnSource = 0;
	long onlyOnDest = 0;
	
	List<BsonValue> failedIds;
	
	private RawBsonDocument chunk;
	
	private Bson chunkQuery;
	
	
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
		builder.append(", matches=");
		builder.append(matches);
		builder.append(", failedIds=");
		builder.append(failedIds);
		builder.append(", chunkQuery");
		builder.append(chunkQuery);
		builder.append("]");
		return builder.toString();
	}

	public Bson getChunkQuery() {
		return chunkQuery;
	}

	public void setChunkQuery(Bson chunkQuery) {
		this.chunkQuery = chunkQuery;
	}
	
	
	
	
	


}
