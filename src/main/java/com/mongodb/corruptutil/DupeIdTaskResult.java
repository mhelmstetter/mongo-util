package com.mongodb.corruptutil;

public class DupeIdTaskResult {
	
	public long totalCount;
	public long dupeCount;
	public long duplicateDocsCount;
	
	public DupeIdTaskResult(long totalCount, long dupeCount, long duplicateDocsCount) {
		this.totalCount = totalCount;
		this.dupeCount = dupeCount;
		this.duplicateDocsCount = duplicateDocsCount;
	}

}
