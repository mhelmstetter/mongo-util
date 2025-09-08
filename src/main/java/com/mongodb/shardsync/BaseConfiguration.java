package com.mongodb.shardsync;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import com.mongodb.model.Namespace;
import com.mongodb.util.DatabaseUtil;

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
	protected List<String> includedNamespaceStrings = new ArrayList<>();
	
	
	protected Set<String> includeDatabases = new HashSet<String>();
	protected Set<String> includeDatabasesAll = new HashSet<String>();
	protected List<String> pendingAmbiguousNamespaces = new ArrayList<>();
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
		if (DatabaseUtil.isSystemDatabase(ns.getDatabaseName())) {
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
			int firstDot = nsStr.indexOf('.');
			if (firstDot == -1) {
				// No dot = database filter
				includeDatabases.add(nsStr);
				includeDatabasesAll.add(nsStr);
			} else {
				// Has dot = collection filter (use same logic as Namespace class)
				includedNamespaceStrings.add(nsStr);
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
				includeDatabasesAll.add(ns.getDatabaseName());
			}
		}
	}
	
	/**
	 * Set include databases (new API)
	 */
	public void setIncludeDatabases(String[] databaseList) {
		if (databaseList == null || databaseList.length == 0) {
			return;
		}
		filtered = true;
		for (String dbName : databaseList) {
			includeDatabases.add(dbName);
			includeDatabasesAll.add(dbName);
		}
	}
	
	/**
	 * Set include namespaces with ambiguity resolution (new API)
	 */
	public void setIncludeNamespaces(String[] namespaceList) {
		if (namespaceList == null || namespaceList.length == 0) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceList) {
			processNamespaceWithAmbiguityResolution(nsStr);
		}
	}
	
	/**
	 * Process namespace with ambiguity resolution for multiple dots
	 */
	private void processNamespaceWithAmbiguityResolution(String namespaceFilter) {
		if (!namespaceFilter.contains(".")) {
			logger.warn("Namespace '{}' contains no dots - did you mean to use --includeDB instead?", namespaceFilter);
			return;
		}
		
		int dotCount = StringUtils.countOccurrencesOf(namespaceFilter, ".");
		
		if (dotCount == 1) {
			// Unambiguous case
			processResolvedNamespace(namespaceFilter);
			return;
		}
		
		// Multiple dots - need to resolve ambiguity
		logger.debug("Resolving ambiguous namespace '{}' with {} dots", namespaceFilter, dotCount);
		
		// For now, we'll implement basic resolution. Later we'll add database lookup
		// when source client is available
		pendingAmbiguousNamespaces.add(namespaceFilter);
		
		// Use first-dot strategy as fallback
		int firstDot = namespaceFilter.indexOf('.');
		String possibleDb = namespaceFilter.substring(0, firstDot);
		String possibleCollection = namespaceFilter.substring(firstDot + 1);
		
		logger.warn("Namespace '{}' is ambiguous. Assuming database='{}', collection='{}'. " +
		           "If this is incorrect, use --includeDB to specify the database explicitly.", 
		           namespaceFilter, possibleDb, possibleCollection);
		           
		processResolvedNamespace(namespaceFilter);
	}
	
	/**
	 * Process a resolved namespace (single dot or already resolved)
	 */
	private void processResolvedNamespace(String namespaceStr) {
		includedNamespaceStrings.add(namespaceStr);
		Namespace ns = new Namespace(namespaceStr);
		includeNamespaces.add(ns);
		includeDatabasesAll.add(ns.getDatabaseName());
	}

	public boolean isFiltered() {
		return filtered;
	}
	
	/**
	 * Resolve ambiguous namespaces using source database information
	 * This should be called after the source client is initialized
	 */
	public void resolveAmbiguousNamespaces() {
		if (pendingAmbiguousNamespaces.isEmpty() || sourceShardClient == null) {
			return;
		}
		
		logger.debug("Resolving {} ambiguous namespace(s)", pendingAmbiguousNamespaces.size());
		
		// Get list of source database names
		Set<String> sourceDatabases = getSourceDatabaseNames();
		
		for (String namespaceFilter : pendingAmbiguousNamespaces) {
			resolveAmbiguousNamespaceWithDatabaseLookup(namespaceFilter, sourceDatabases);
		}
		
		pendingAmbiguousNamespaces.clear();
	}
	
	/**
	 * Get source database names for ambiguity resolution
	 */
	private Set<String> getSourceDatabaseNames() {
		Set<String> databaseNames = new HashSet<>();
		try {
			// Use the databases collection from config database (for sharded clusters)
			if (sourceShardClient.getDatabasesCollection() != null) {
				sourceShardClient.getDatabasesCollection().find().forEach(doc -> {
					String dbName = doc.getString("_id");
					if (dbName != null) {
						databaseNames.add(dbName);
					}
				});
			} else {
				// Fallback to listDatabases command
				org.bson.Document result = sourceShardClient.listDatabases();
				if (result != null) {
					@SuppressWarnings("unchecked")
					List<org.bson.Document> databases = (List<org.bson.Document>) result.get("databases");
					if (databases != null) {
						for (org.bson.Document db : databases) {
							String dbName = db.getString("name");
							if (dbName != null) {
								databaseNames.add(dbName);
							}
						}
					}
				}
			}
		} catch (Exception e) {
			logger.warn("Failed to get source database names for ambiguity resolution: {}", e.getMessage());
		}
		return databaseNames;
	}
	
	/**
	 * Resolve ambiguous namespace using database lookup
	 */
	private void resolveAmbiguousNamespaceWithDatabaseLookup(String namespaceFilter, Set<String> sourceDatabases) {
		List<String> possibleSplits = generatePossibleDatabaseSplits(namespaceFilter);
		
		String resolvedDatabase = null;
		String resolvedCollection = null;
		
		// Try each possible split and see which database exists on source
		for (String possibleDb : possibleSplits) {
			if (sourceDatabases.contains(possibleDb)) {
				resolvedDatabase = possibleDb;
				resolvedCollection = namespaceFilter.substring(possibleDb.length() + 1);
				logger.info("Resolved ambiguous namespace '{}' as database='{}', collection='{}'", 
				           namespaceFilter, resolvedDatabase, resolvedCollection);
				break;
			}
		}
		
		if (resolvedDatabase == null) {
			// No match found - warn user and use first-dot strategy
			int firstDot = namespaceFilter.indexOf('.');
			resolvedDatabase = namespaceFilter.substring(0, firstDot);
			resolvedCollection = namespaceFilter.substring(firstDot + 1);
			logger.warn("Could not resolve ambiguous namespace '{}' - no matching database found in source cluster. " +
			           "Using first-dot strategy: database='{}', collection='{}'", 
			           namespaceFilter, resolvedDatabase, resolvedCollection);
		}
		
		// The namespace was already processed with first-dot strategy as fallback
		// No need to add it again since processNamespaceWithAmbiguityResolution already called processResolvedNamespace
	}
	
	/**
	 * Generate possible database name splits for ambiguous namespace
	 */
	private List<String> generatePossibleDatabaseSplits(String namespace) {
		List<String> possibilities = new ArrayList<>();
		int lastDot = namespace.lastIndexOf('.');
		
		// Generate all possible splits from first dot to second-to-last dot
		for (int i = namespace.indexOf('.'); i < lastDot; i = namespace.indexOf('.', i + 1)) {
			possibilities.add(namespace.substring(0, i));
		}
		
		// Add the possibility that uses all but the last segment as database name
		possibilities.add(namespace.substring(0, lastDot));
		
		return possibilities;
	}

	public void setFiltered(boolean filtered) {
		this.filtered = filtered;
	}

	public Set<Namespace> getIncludeNamespaces() {
		return includeNamespaces;
	}
	
	public List<String> getIncludedNamespaceStrings() {
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
		if (destShardClient == null && destClusterUri != null) {
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
			
			// Resolve any pending ambiguous namespaces now that we have database information
			resolveAmbiguousNamespaces();
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