package com.mongodb.mongoreplay;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class ReplayAccumulator {
    
    private String dbName;
    private String collName;
    private String shape;
    DescriptiveStatistics executionStats = new DescriptiveStatistics();

}
