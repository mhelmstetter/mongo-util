package com.mongodb.atlas.model;

import java.util.List;

public class ProjectsResult {
    
    private Integer totalCount;
    
    private List<Project> results;
    
    public List<Project> getProjects() {
        return results;
    }

}
