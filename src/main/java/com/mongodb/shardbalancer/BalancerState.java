package com.mongodb.shardbalancer;

public class BalancerState {
	
	private boolean runAnalyzer;

	public boolean isRunAnalyzer() {
		return runAnalyzer;
	}

	public void setRunAnalyzer(boolean analyzerRequested) {
		this.runAnalyzer = analyzerRequested;
	}
	
	

}
