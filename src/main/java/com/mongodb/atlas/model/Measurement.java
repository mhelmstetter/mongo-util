package com.mongodb.atlas.model;

import java.util.List;

public class Measurement {
    
    private String name;
    
    private String units;
    
    private List<MeasurementDataPoint> dataPoints;

    public List<MeasurementDataPoint> getDataPoints() {
        return dataPoints;
    }

    public String getName() {
        return name;
    }

    public String getUnits() {
        return units;
    }
    
    

}
