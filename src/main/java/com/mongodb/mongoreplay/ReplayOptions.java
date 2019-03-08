package com.mongodb.mongoreplay;

import java.util.HashSet;
import java.util.Set;

import org.bson.BsonDocument;

public class ReplayOptions {
    
    private Set<String> ignoredCollections = new HashSet<String>();
    protected String[] removeUpdateFields;
    
    private BsonDocument writeConcern;

    public Set<String> getIgnoredCollections() {
        return ignoredCollections;
    }

    public void setIgnoredCollections(Set<String> ignoredCollections) {
        this.ignoredCollections = ignoredCollections;
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
    
    

}
