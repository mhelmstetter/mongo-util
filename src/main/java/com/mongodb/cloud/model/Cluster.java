package com.mongodb.cloud.model;

public class Cluster {

    private String groupId;
    private String typeName;
    private String clusterName;
    private String shardName;
    private String replicaSetName;

    public String getGroupId() {
        return groupId;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getClusterName() {
        return clusterName;
    }

    public String getShardName() {
        return shardName;
    }

    public String getReplicaSetName() {
        return replicaSetName;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Cluster [groupId=");
        builder.append(groupId);
        builder.append(", typeName=");
        builder.append(typeName);
        builder.append(", clusterName=");
        builder.append(clusterName);
        builder.append(", shardName=");
        builder.append(shardName);
        builder.append(", replicaSetName=");
        builder.append(replicaSetName);
        builder.append("]");
        return builder.toString();
    }

}
