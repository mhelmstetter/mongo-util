package com.mongodb.atlas.model;

public class LogCollectionJobRequest {

	private String resourceType;
	private String resourceName;
	private String[] logTypes;
	private boolean redacted = true;
    private int sizeRequestedPerFileBytes = 10000000;

	public String getResourceType() {
		return resourceType;
	}

	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}

	public String getResourceName() {
		return resourceName;
	}

	public void setResourceName(String resourceName) {
		this.resourceName = resourceName;
	}

	public String[] getLogTypes() {
		return logTypes;
	}

	public void setLogTypes(String[] logTypes) {
		this.logTypes = logTypes;
	}

	public boolean isRedacted() {
		return redacted;
	}

	public void setRedacted(boolean redacted) {
		this.redacted = redacted;
	}

	public int getSizeRequestedPerFileBytes() {
		return sizeRequestedPerFileBytes;
	}

	public void setSizeRequestedPerFileBytes(int sizeRequestedPerFileBytes) {
		this.sizeRequestedPerFileBytes = sizeRequestedPerFileBytes;
	}

}
