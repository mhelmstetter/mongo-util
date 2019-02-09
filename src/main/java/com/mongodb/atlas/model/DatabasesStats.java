package com.mongodb.atlas.model;

public class DatabasesStats {
    
    private double totalDataSize = 0.0;
    private double totalIndexSize = 0.0;
    
    public void addDataSize(double d) {
        totalDataSize += d;
    }
    
    public void addIndexSize(double d) {
        totalIndexSize += d;
    }

    public double getTotalDataSize() {
        return totalDataSize;
    }
    
    public double getTotalIndexSize() {
        return totalIndexSize;
    }

}
