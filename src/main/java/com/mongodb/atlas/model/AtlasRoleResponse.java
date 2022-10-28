package com.mongodb.atlas.model;

public class AtlasRoleResponse {

	private boolean success;
	private boolean duplicate;
	private AtlasRole response;
	private String responseError;
	
	private AtlasRoleResponse(boolean success) {
		this.success = success;
	}
	
	public static AtlasRoleResponse newFailedResponse(String responseError) {
		AtlasRoleResponse response = new AtlasRoleResponse(false);
		response.setResponseError(responseError);
		return response;
	}
	
	public static AtlasRoleResponse newSuccessResponse(AtlasRole atlasRole) {
		AtlasRoleResponse response = new AtlasRoleResponse(true);
		response.setResponse(atlasRole);
		return response;
	}
	
	public static AtlasRoleResponse newDuplicateResponse(String responseError) {
		AtlasRoleResponse response = new AtlasRoleResponse(false);
		response.setResponseError(responseError);
		response.setDuplicate(true);
		return response;
	}

	public boolean isSuccess() {
		return success;
	}

	public void setSuccess(boolean success) {
		this.success = success;
	}

	public AtlasRole getResponse() {
		return response;
	}

	public void setResponse(AtlasRole response) {
		this.response = response;
	}

	public String getResponseError() {
		return responseError;
	}

	public void setResponseError(String responseError) {
		this.responseError = responseError;
	}

	public boolean isDuplicate() {
		return duplicate;
	}

	public void setDuplicate(boolean duplicate) {
		this.duplicate = duplicate;
	}

}
