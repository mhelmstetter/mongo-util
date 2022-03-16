package com.mongodb.mongoreplay;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.BsonDocument;
import org.bson.BsonString;

import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

public class ReplayOptions {
    
    private Set<String> ignoredCollections = new HashSet<String>();
    protected String[] removeUpdateFields;
    
    private Map<String, String> dbNamesMap;
    
    private ReplayMode replayMode;
    
    private BsonDocument writeConcern = WriteConcern.ACKNOWLEDGED.asDocument();
    private Long sleepMillis;
    
    private BsonDocument readConcernDocument;
    
    private ReadPreference readPreference;
    
    // readConcern: { level: "majority" }
    
    public ReadPreference getReadPreference() {
		return readPreference;
	}

	public void setReadPreference(ReadPreference readPreference) {
		this.readPreference = readPreference;
	}

	public ReplayOptions() {
    	ignoredCollections.add("system.sessions");
    }

    public Set<String> getIgnoredCollections() {
        return ignoredCollections;
    }

    public void addIgnoredCollections(Set<String> ignoredCollections) {
        this.ignoredCollections.addAll(ignoredCollections);
    }

    public String[] getRemoveUpdateFields() {
        return removeUpdateFields;
    }

    public void setRemoveUpdateFields(String[] removeUpdateFields) {
        this.removeUpdateFields = removeUpdateFields;
    }

    public BsonDocument getWriteConcern() {
        return writeConcern;
    }

    public void setWriteConcern(BsonDocument writeConcern) {
        this.writeConcern = writeConcern;
    }
    
    public void setReadConcernLevel(ReadConcernLevel level) {
        this.readConcernDocument = new BsonDocument();
        readConcernDocument.put("level", new BsonString(level.getValue()));
    }

    public BsonDocument getReadConcern() {
        return readConcernDocument;
    }

	public Long getSleepMillis() {
		return sleepMillis;
	}

	public void setSleepMillis(Long sleepMillis) {
		this.sleepMillis = sleepMillis;
	}


	public void setDbNameMapString(String dbNamesMapString) {
		dbNamesMap = new HashMap<>();
		
		String[] mappings = dbNamesMapString.split(",");
		
		for (String mapping : mappings) {
			String[] dbMapping = mapping.split("\\|");
			dbNamesMap.put(dbMapping[0], dbMapping[1]);
		}
		// TODO Auto-generated method stub
		
	}

	public Map<String, String> getDbNamesMap() {
		return dbNamesMap;
	}

	public ReplayMode getReplayMode() {
		return replayMode;
	}

	public void setReplayMode(ReplayMode replayMode) {
		this.replayMode = replayMode;
	}

}
