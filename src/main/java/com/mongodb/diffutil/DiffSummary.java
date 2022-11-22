package com.mongodb.diffutil;

import java.util.Objects;

public class DiffSummary {
	
	public long totalDbs = 0;
	public long missingDbs = 0;
	public long totalCollections = 0;
	public long totalMatches = 0;
	public long totalMissingDocs = 0;
	public long totalKeysMisordered = 0;
	public long totalHashMismatched = 0;
	
	@Override
	public int hashCode() {
		return Objects.hash(missingDbs, totalCollections, totalDbs, totalHashMismatched, totalKeysMisordered,
				totalMatches, totalMissingDocs);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DiffSummary other = (DiffSummary) obj;
		return missingDbs == other.missingDbs && totalCollections == other.totalCollections
				&& totalDbs == other.totalDbs && totalHashMismatched == other.totalHashMismatched
				&& totalKeysMisordered == other.totalKeysMisordered && totalMatches == other.totalMatches
				&& totalMissingDocs == other.totalMissingDocs;
	}
	
	
	

}
