package com.mongodb.shardsync;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Namespace;

public class BaseConfiguration {

	protected static Logger logger = LoggerFactory.getLogger(BaseConfiguration.class);
	
	public class Constants {
		public final static String SOURCE_URI = "source";
	    public final static String DEST_URI = "dest";
	}
    
    
	private ShardClient destShardClient;
	private ShardClient sourceShardClient;
	public String sourceClusterUri;
	public String destClusterUri;
	public boolean shardToRs;
	public boolean filtered;
	protected Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	protected Set<String> includedNamespaceStrings = new HashSet<String>();
	
	
	protected Set<String> includeDatabases = new HashSet<String>();
	protected Set<String> includeDatabasesAll = new HashSet<String>();
	public String[] shardMap;
	
	public String destCsrsUri;
	
	public String sourceClusterPattern;
	public String destClusterPattern;
	public String sourceRsPattern;
	public String destRsPattern;
	public String[] sourceRsManual;
	public String[] destRsManual;
	public String sourceRsRegex;
	public String destRsRegex;
	
	public Boolean sourceRsSsl;

	public BaseConfiguration() {
		super();
	}
	
	public boolean filterCheck(String nsStr) {
		Namespace ns = new Namespace(nsStr);
		return filterCheck(ns);
	}
	
	public boolean filterCheck(Namespace ns) {
		if (isFiltered() && !includeNamespaces.contains(ns) && !includeDatabases.contains(ns.getDatabaseName())) {
			logger.trace("Namespace " + ns + " filtered, skipping");
			return true;
		}
		if (ns.getDatabaseName().equals("config") || ns.getDatabaseName().equals("admin") || ns.getDatabaseName().equals("local")) {
			return true;
		}
		if (ns.getCollectionName().equals("system.profile") || ns.getCollectionName().equals("system.users")) {
			return true;
		}
		return false;
	}
	
	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null || namespaceFilterList.length == 0) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				includedNamespaceStrings.add(nsStr);
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
				includeDatabasesAll.add(ns.getDatabaseName());
			} else {
				includeDatabases.add(nsStr);
				includeDatabasesAll.add(nsStr);
			}
		}
	}

	public boolean isFiltered() {
		return filtered;
	}

	public void setFiltered(boolean filtered) {
		this.filtered = filtered;
	}

	public Set<Namespace> getIncludeNamespaces() {
		return includeNamespaces;
	}
	
	public Set<String> getIncludedNamespaceStrings() {
		return includedNamespaceStrings;
	}

	public void setIncludeNamespaces(Set<Namespace> includeNamespaces) {
		this.includeNamespaces = includeNamespaces;
	}

	public Set<String> getIncludeDatabases() {
		return includeDatabases;
	}

	public void setIncludeDatabases(Set<String> includeDatabases) {
		this.includeDatabases = includeDatabases;
	}

	public Set<String> getIncludeDatabasesAll() {
		return includeDatabasesAll;
	}

	public void setIncludeDatabasesAll(Set<String> includeDatabasesAll) {
		this.includeDatabasesAll = includeDatabasesAll;
	}

	public String[] getShardMap() {
		return shardMap;
	}

	public void setShardMap(String[] shardMap) {
		this.shardMap = shardMap;
	}

	public ShardClient getDestShardClient() {
		if (destShardClient == null) {
			destShardClient = new ShardClient("dest", destClusterUri);
			destShardClient.init();
		}
		return destShardClient;
	}

	public void setDestShardClient(ShardClient destShardClient) {
		this.destShardClient = destShardClient;
	}

	public ShardClient getSourceShardClient() {
		if (sourceShardClient == null) {
			sourceShardClient = new ShardClient("source", sourceClusterUri);
			sourceShardClient.init();
		}
		return sourceShardClient;
	}

	public void setSourceShardClient(ShardClient sourceShardClient) {
		this.sourceShardClient = sourceShardClient;
	}

	public String getSourceRsRegex() {
		return sourceRsRegex;
	}

	public void setSourceRsRegex(String sourceRsRegex) {
		this.sourceRsRegex = sourceRsRegex;
	}

	public String getDestRsRegex() {
		return destRsRegex;
	}

	public void setDestRsRegex(String destRsRegex) {
		this.destRsRegex = destRsRegex;
	}
	
	public String getSourceClusterUri() {
		return sourceClusterUri;
	}

	public void setSourceClusterUri(String sourceClusterUri) {
		this.sourceClusterUri = sourceClusterUri;
	}

	public String getDestClusterUri() {
		return destClusterUri;
	}

	public void setDestClusterUri(String destClusterUri) {
		this.destClusterUri = destClusterUri;
	}

	public String getSourceClusterPattern() {
		return sourceClusterPattern;
	}

	public void setSourceClusterPattern(String sourceClusterPattern) {
		this.sourceClusterPattern = sourceClusterPattern;
	}

	public String getDestClusterPattern() {
		return destClusterPattern;
	}

	public void setDestClusterPattern(String destClusterPattern) {
		this.destClusterPattern = destClusterPattern;
	}

	public String getSourceRsPattern() {
		return sourceRsPattern;
	}

	public void setSourceRsPattern(String sourceRsPattern) {
		this.sourceRsPattern = sourceRsPattern;
	}

	public String getDestRsPattern() {
		return destRsPattern;
	}

	public void setDestRsPattern(String destRsPattern) {
		this.destRsPattern = destRsPattern;
	}

	public String[] getSourceRsManual() {
		return sourceRsManual;
	}

	public void setSourceRsManual(String[] sourceRsManual) {
		this.sourceRsManual = sourceRsManual;
	}

	public String[] getDestRsManual() {
		return destRsManual;
	}

	public void setDestRsManual(String[] destRsManual) {
		this.destRsManual = destRsManual;
	}
	
	public String getDestCsrsUri() {
		return destCsrsUri;
	}

	public void setDestCsrsUri(String destCsrsUri) {
		this.destCsrsUri = destCsrsUri;
	}
	
	public boolean isShardToRs() {
		return shardToRs;
	}

	public void setShardToRs(boolean shardToRs) {
		this.shardToRs = shardToRs;
	}

	public Boolean getSourceRsSsl() {
		return sourceRsSsl;
	}

	public void setSourceRsSsl(Boolean sourceRsSsl) {
		this.sourceRsSsl = sourceRsSsl;
	}

}