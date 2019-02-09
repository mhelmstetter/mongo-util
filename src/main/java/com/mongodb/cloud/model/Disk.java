package com.mongodb.cloud.model;

public class Disk {
    
    private String partitionName;

    public String getPartitionName() {
        return partitionName;
    }

    public void setPartitionName(String partitionName) {
        this.partitionName = partitionName;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Disk [partitionName=");
        builder.append(partitionName);
        builder.append("]");
        return builder.toString();
    }

}
