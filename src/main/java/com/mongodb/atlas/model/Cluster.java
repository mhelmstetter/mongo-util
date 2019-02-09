package com.mongodb.atlas.model;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

public class Cluster {

    private Boolean backupEnabled;
    private String clusterType;
    private Integer diskSizeGB;
    private String encryptionAtRestProvider;
    private String groupId;
    private String mongoDBVersion;
    private String mongoURI;
    private String mongoURIUpdated;
    private String mongoURIWithOptions;
    private String name;
    private Integer numShards;
    private Boolean paused;
    private ProviderSettings providerSettings;
    private Integer replicationFactor;
    private String stateName;

    public Boolean getBackupEnabled() {
        return backupEnabled;
    }

    public void setBackupEnabled(Boolean backupEnabled) {
        this.backupEnabled = backupEnabled;
    }

    public String getClusterType() {
        return clusterType;
    }

    public void setClusterType(String clusterType) {
        this.clusterType = clusterType;
    }

    public Integer getDiskSizeGB() {
        return diskSizeGB;
    }

    public void setDiskSizeGB(Integer diskSizeGB) {
        this.diskSizeGB = diskSizeGB;
    }

    public String getEncryptionAtRestProvider() {
        return encryptionAtRestProvider;
    }

    public void setEncryptionAtRestProvider(String encryptionAtRestProvider) {
        this.encryptionAtRestProvider = encryptionAtRestProvider;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getMongoDBVersion() {
        return mongoDBVersion;
    }

    public void setMongoDBVersion(String mongoDBVersion) {
        this.mongoDBVersion = mongoDBVersion;
    }

    public String getMongoURI() {
        return mongoURI;
    }

    public void setMongoURI(String mongoURI) {
        this.mongoURI = mongoURI;
    }

    public String getMongoURIUpdated() {
        return mongoURIUpdated;
    }

    public void setMongoURIUpdated(String mongoURIUpdated) {
        this.mongoURIUpdated = mongoURIUpdated;
    }

    public String getMongoURIWithOptions() {
        return mongoURIWithOptions;
    }

    public void setMongoURIWithOptions(String mongoURIWithOptions) {
        this.mongoURIWithOptions = mongoURIWithOptions;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getNumShards() {
        return numShards;
    }

    public void setNumShards(Integer numShards) {
        this.numShards = numShards;
    }

    public Boolean getPaused() {
        return paused;
    }

    public void setPaused(Boolean paused) {
        this.paused = paused;
    }

    public ProviderSettings getProviderSettings() {
        return providerSettings;
    }

    public void setProviderSettings(ProviderSettings providerSettings) {
        this.providerSettings = providerSettings;
    }

    public Integer getReplicationFactor() {
        return replicationFactor;
    }

    public void setReplicationFactor(Integer replicationFactor) {
        this.replicationFactor = replicationFactor;
    }

    public String getStateName() {
        return stateName;
    }

    public void setStateName(String stateName) {
        this.stateName = stateName;
    }

    
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(clusterType).append(backupEnabled).append(paused)
                .append(encryptionAtRestProvider).append(groupId).append(numShards).append(mongoDBVersion)
                .append(stateName).append(name).append(mongoURI).append(replicationFactor).append(diskSizeGB)
                .append(mongoURIWithOptions).append(providerSettings).append(mongoURIUpdated).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof Cluster) == false) {
            return false;
        }
        Cluster rhs = ((Cluster) other);
        return new EqualsBuilder().append(clusterType, rhs.clusterType).append(backupEnabled, rhs.backupEnabled)
                .append(paused, rhs.paused).append(encryptionAtRestProvider, rhs.encryptionAtRestProvider)
                .append(groupId, rhs.groupId).append(numShards, rhs.numShards)
                .append(mongoDBVersion, rhs.mongoDBVersion).append(stateName, rhs.stateName).append(name, rhs.name)
                .append(mongoURI, rhs.mongoURI).append(replicationFactor, rhs.replicationFactor)
                .append(diskSizeGB, rhs.diskSizeGB).append(mongoURIWithOptions, rhs.mongoURIWithOptions)
                .append(providerSettings, rhs.providerSettings).append(mongoURIUpdated, rhs.mongoURIUpdated).isEquals();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Cluster [");
        //builder.append("clusterType=");
        //builder.append(clusterType);
        builder.append(", diskSizeGB=");
        builder.append(diskSizeGB);
        builder.append(", mongoDBVersion=");
        builder.append(mongoDBVersion);
        builder.append(", name=");
        builder.append(name);
        builder.append(", ");
        builder.append(providerSettings);
        //builder.append(", numShards=");
        //builder.append(numShards);
        //builder.append(", stateName=");
        //builder.append(stateName);
        builder.append("]");
        return builder.toString();
    }

}