package com.mongodb.model;

import java.util.HashMap;
import java.util.Map;

public class DatabaseCatalog {
	
	private Map<String, Database> databases;
	
	private Long documentCount = 0L;

	public void addDatabase(Database db) {
		if (this.databases == null) {
			databases = new HashMap<>();
		}
		databases.put(db.getName(), db);
		documentCount += db.getDbStats().getDocumentCount();
	}

	public Long getDocumentCount() {
		return documentCount;
	}

}
