package com.mongodb.diff3;

public class DiffResult {
	
	long missing = 0;
	long matches = 0;
	long keysMisordered = 0;
	long hashMismatched = 0;
	long total = 0;
	
	private String shardName;
	
	
	public String getShardName() {
		return shardName;
	}
	public void setShardName(String shardName) {
		this.shardName = shardName;
	}
	
	public void incrementMatches() {
		matches++;
	}
	
	public void incrementMissing() {
		missing++;
	}
	
	public void incrementTotal() {
		total++;
	}
	
	public void incrementKeysMisordered() {
		keysMisordered++;
	}
	
	public void incrementHashMismatched() {
		hashMismatched++;
	}


}
