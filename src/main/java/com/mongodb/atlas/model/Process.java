package com.mongodb.atlas.model;

public class Process {

    private String id;
    private String hostname;
    private String typeName;
    private String version;
    private String replicaSetName;

    public String getId() {
        return id;
    }

    public String getHostname() {
        return hostname;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getVersion() {
        return version;
    }

    public String getReplicaSetName() {
        return replicaSetName;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Process [id=");
        builder.append(id);
        builder.append("]");
        return builder.toString();
    }

}
