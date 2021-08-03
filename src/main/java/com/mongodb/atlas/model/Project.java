package com.mongodb.atlas.model;

public class Project {

	private Integer clusterCount;
	private String name;
	private String id;

	public Integer getClusterCount() {
		return clusterCount;
	}

	public void setClusterCount(Integer clusterCount) {
		this.clusterCount = clusterCount;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Project [clusterCount=");
		builder.append(clusterCount);
		builder.append(", name=");
		builder.append(name);
		builder.append(", id=");
		builder.append(id);
		builder.append("]");
		return builder.toString();
	}

}
