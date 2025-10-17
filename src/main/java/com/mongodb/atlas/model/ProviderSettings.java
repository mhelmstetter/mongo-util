package com.mongodb.atlas.model;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class ProviderSettings {

    private String providerName;
    private Integer diskIOPS;
    private Boolean encryptEBSVolume;
    private String instanceSizeName;
    private String regionName;

    public String getProviderName() {
        return providerName;
    }

    public void setProviderName(String providerName) {
        this.providerName = providerName;
    }

    public Integer getDiskIOPS() {
        return diskIOPS;
    }

    public void setDiskIOPS(Integer diskIOPS) {
        this.diskIOPS = diskIOPS;
    }

    public Boolean getEncryptEBSVolume() {
        return encryptEBSVolume;
    }

    public void setEncryptEBSVolume(Boolean encryptEBSVolume) {
        this.encryptEBSVolume = encryptEBSVolume;
    }

    public String getInstanceSizeName() {
        return instanceSizeName;
    }

    public void setInstanceSizeName(String instanceSizeName) {
        this.instanceSizeName = instanceSizeName;
    }

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(instanceSizeName).append(diskIOPS).append(providerName).append(regionName)
                .append(encryptEBSVolume).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if ((other instanceof ProviderSettings) == false) {
            return false;
        }
        ProviderSettings rhs = ((ProviderSettings) other);
        return new EqualsBuilder().append(instanceSizeName, rhs.instanceSizeName).append(diskIOPS, rhs.diskIOPS)
                .append(providerName, rhs.providerName).append(regionName, rhs.regionName)
                .append(encryptEBSVolume, rhs.encryptEBSVolume).isEquals();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("ProviderSettings [diskIOPS=");
        builder.append(diskIOPS);
        builder.append(", instanceSizeName=");
        builder.append(instanceSizeName);
        builder.append("]");
        return builder.toString();
    }

}