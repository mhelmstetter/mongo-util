package com.mongodb.mongosync;

import java.io.IOException;

public interface MongoSyncPauseListener {
	
	public void mongoSyncPaused(MongoSyncRunner runner) throws IOException;

}
