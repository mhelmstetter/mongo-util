package com.mongodb.cloud.model;

import java.util.List;

public class DisksResult {
    
    private Integer totalCount;
    
    private List<Disk> results;
    
    public List<Disk> getDisks() {
        return results;
    }

}
