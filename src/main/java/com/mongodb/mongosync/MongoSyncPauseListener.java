package com.mongodb.mongosync;

public interface MongoSyncPauseListener {
	
	public void mongoSyncPaused(MongoSyncRunner runner);

}
