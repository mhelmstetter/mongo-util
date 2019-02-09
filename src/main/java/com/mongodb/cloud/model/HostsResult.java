package com.mongodb.cloud.model;

import java.util.List;

public class HostsResult {
    
    private Integer totalCount;
    
    private List<Host> results;
    
    public List<Host> getHosts() {
        return results;
    }

}
