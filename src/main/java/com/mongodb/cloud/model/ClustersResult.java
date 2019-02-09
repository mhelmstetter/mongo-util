package com.mongodb.cloud.model;

import java.util.List;

public class ClustersResult {
    
    private Integer totalCount;
    
    private List<Cluster> results;
    
    public List<Cluster> getClusters() {
        return results;
    }

}
