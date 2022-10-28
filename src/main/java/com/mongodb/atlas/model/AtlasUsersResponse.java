package com.mongodb.atlas.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class AtlasUsersResponse {
	
	private List<AtlasUser> results;

	public List<AtlasUser> getResults() {
		return results;
	}

	public void setResults(List<AtlasUser> results) {
		this.results = results;
	}
	
	
	

}
