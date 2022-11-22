package com.mongodb.model;

import java.util.List;

public class ReplicaSetInfo {

	private List<String> hosts;
	private String rsName;
	private boolean writablePrimary;
	private boolean secondary;

	public List<String> getHosts() {
		return hosts;
	}

	public void setHosts(List<String> hosts) {
		this.hosts = hosts;
	}

	public String getRsName() {
		return rsName;
	}

	public void setRsName(String rsName) {
		this.rsName = rsName;
	}

	public boolean isWritablePrimary() {
		return writablePrimary;
	}

	public void setWritablePrimary(boolean writablePrimary) {
		this.writablePrimary = writablePrimary;
	}

	public boolean isSecondary() {
		return secondary;
	}

	public void setSecondary(boolean secondary) {
		this.secondary = secondary;
	}

}
