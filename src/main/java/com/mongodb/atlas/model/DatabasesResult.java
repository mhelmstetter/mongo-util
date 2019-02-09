package com.mongodb.atlas.model;

import java.util.List;

public class DatabasesResult {
    
    private Integer totalCount;
    
    private List<Database> results;
    
    public List<Database> getDatabases() {
        return results;
    }

}
