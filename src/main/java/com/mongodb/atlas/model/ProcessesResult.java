package com.mongodb.atlas.model;

import java.util.List;

public class ProcessesResult {
    
    private Integer totalCount;
    
    private List<Process> results;
    
    public List<Process> getProcesses() {
        return results;
    }
    
    public Integer getTotalCount() {
        return totalCount;
    }
}
