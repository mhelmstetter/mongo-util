package com.mongodb.model;

public class OplogSummary {

	private Object id;
	private Namespace ns;
	private OplogOpType opType;

	public OplogSummary(Namespace ns, Object id, OplogOpType opType) {
		this.ns = ns;
		this.id = id;
		this.opType = opType;
	}

	public Object getId() {
		return id;
	}

	public void setId(Object id) {
		this.id = id;
	}

	public Namespace getNs() {
		return ns;
	}

	public void setNs(Namespace ns) {
		this.ns = ns;
	}

	public OplogOpType getOpType() {
		return opType;
	}

	public void setOpType(OplogOpType opType) {
		this.opType = opType;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("OplogSummary [id=");
		builder.append(id);
		builder.append(", ns=");
		builder.append(ns);
		builder.append(", opType=");
		builder.append(opType);
		builder.append("]");
		return builder.toString();
	}

}
