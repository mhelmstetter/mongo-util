package com.mongodb.model;

import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.Maps;

public enum OplogOpType {

	INSERT("insert", "i"), UPDATE("update", "u"), DELETE("delete", "d"), NO_OP("no-op", "n"), CREATE("create", "c");

	private String label;
	private String code;

	private static final Map<String, OplogOpType> codeLookup = Maps.uniqueIndex(Arrays.asList(OplogOpType.values()),
			OplogOpType::getCode);

	OplogOpType(String label, String code) {
		this.label = label;
		this.code = code;
	}

	public String getLabel() {
		return label;
	}

	public String getCode() {
		return code;
	}

	public static OplogOpType fromCode(String code) {
		return codeLookup.get(code);
	}

}
