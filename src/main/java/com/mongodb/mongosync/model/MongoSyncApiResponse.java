package com.mongodb.mongosync.model;

public class MongoSyncApiResponse {

	private boolean success;
	private String error;
	private String errorDescription;

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getErrorDescription() {
		return errorDescription;
	}

	public void setErrorDescription(String errorDescription) {
		this.errorDescription = errorDescription;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MongoSyncApiResponse [success=");
		builder.append(success);
		builder.append(", error=");
		builder.append(error);
		builder.append(", errorDescription=");
		builder.append(errorDescription);
		builder.append("]");
		return builder.toString();
	}

}
