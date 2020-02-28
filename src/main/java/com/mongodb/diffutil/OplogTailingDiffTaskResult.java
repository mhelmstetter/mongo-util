package com.mongodb.diffutil;

public class OplogTailingDiffTaskResult extends DiffResult {
	
	public OplogTailingDiffTaskResult(String shardName) {
		super(shardName);
	}

	public void addDiffResult(DiffResult diffResult) {
		
		missing += diffResult.missing;
		matches += diffResult.matches;
		keysMisordered += diffResult.keysMisordered;
		hashMismatched += diffResult.hashMismatched;
		total += diffResult.total;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("OplogTailingDiffTaskResult [missing=");
		builder.append(missing);
		builder.append(", matches=");
		builder.append(matches);
		builder.append(", keysMisordered=");
		builder.append(keysMisordered);
		builder.append(", hashMismatched=");
		builder.append(hashMismatched);
		builder.append(", total=");
		builder.append(total);
		builder.append("]");
		return builder.toString();
	}
	
	

}
