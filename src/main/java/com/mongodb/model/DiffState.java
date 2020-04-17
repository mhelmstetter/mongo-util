package com.mongodb.model;

public enum DiffState {

	MISSING_ON_SOURCE("missing on source"), MISSING_ON_DEST("missing on dest"), MISMATCHED_HASH("mismatched hash");

	private String label;

	DiffState(String label) {
		this.label = label;
	}

	public String getLabel() {
		return label;
	}
}
