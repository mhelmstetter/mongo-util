package com.mongodb.atlas.model;

public class MeasurementDataPoint {
    
    private String timestamp;
    
    private Double value;

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MeasurementDataPoint [timestamp=");
        builder.append(timestamp);
        builder.append(", value=");
        builder.append(value);
        builder.append("]");
        return builder.toString();
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Double getValue() {
        return value;
    }

}
