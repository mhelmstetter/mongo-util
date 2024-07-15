package com.mongodb.corruptutil;

public class DupeIdTaskResult {
	
	public long totalCount;
	public long dupeCount;
	
	public DupeIdTaskResult(long totalCount, long dupeCount) {
		this.totalCount = totalCount;
		this.dupeCount = dupeCount;
	}

}
