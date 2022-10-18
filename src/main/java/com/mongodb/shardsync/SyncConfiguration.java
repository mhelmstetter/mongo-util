package com.mongodb.shardsync;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.model.Namespace;

public class SyncConfiguration {
	
	private static Logger logger = LoggerFactory.getLogger(ChunkManager.class);
	
	private ShardClient destShardClient;
	private ShardClient sourceShardClient;
	
	
	public String atlasApiPublicKey;
	public String atlasApiPrivateKey;
	public String atlasProjectId;
	public String sourceClusterUri;
	public String destClusterUri;
	public String sourceClusterPattern;
	public String destClusterPattern;
	public String sourceRsPattern;
	public String destRsPattern;
	public String[] sourceRsManual;
	public String[] destRsManual;
	public String destCsrsUri;
	public boolean dropDestDbs;
	public boolean dropDestDbsAndConfigMetadata;
	public boolean nonPrivilegedMode;
	public boolean doChunkCounts;
	public boolean preserveUUIDs;
	public String compressors;
	public String oplogBasePath;
	public String bookmarkFilePrefix;
	public boolean reverseSync;
	public boolean noIndexRestore;
	public Integer collStatsThreshold;
	public boolean dryRun;
	public boolean shardToRs;
	public boolean extendTtl;
	public boolean filtered;
	
	private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	private Set<String> includeDatabases = new HashSet<String>();
	
	// ugly, but we need a set of includeDatabases that we pass to mongomirror
	// vs. the includes that we use elsewhere
	private Set<String> includeDatabasesAll = new HashSet<String>();
	
	public String[] shardMap;
	public File mongomirrorBinary;
	public long sleepMillis;
	public String numParallelCollections;
	public int mongoMirrorStartPort = 9001;
	public String writeConcern;
	public Long cleanupOrphansSleepMillis;
	public String destVersion;
	public List<Integer> destVersionArray;
	public boolean sslAllowInvalidHostnames;
	public boolean sslAllowInvalidCertificates;
	public boolean skipFlushRouterConfig;

	public SyncConfiguration() {

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
		if (ns.getDatabaseName().equals("config") || ns.getDatabaseName().equals("admin")) {
			return true;
		}
		if (ns.getCollectionName().equals("system.profile") || ns.getCollectionName().equals("system.users")) {
			return true;
		}
		return false;
	}

	public String getAtlasApiPublicKey() {
		return atlasApiPublicKey;
	}

	public void setAtlasApiPublicKey(String atlasApiPublicKey) {
		this.atlasApiPublicKey = atlasApiPublicKey;
	}

	public String getAtlasApiPrivateKey() {
		return atlasApiPrivateKey;
	}

	public void setAtlasApiPrivateKey(String atlasApiPrivateKey) {
		this.atlasApiPrivateKey = atlasApiPrivateKey;
	}

	public String getAtlasProjectId() {
		return atlasProjectId;
	}

	public void setAtlasProjectId(String atlasProjectId) {
		this.atlasProjectId = atlasProjectId;
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

	public boolean isDropDestDbs() {
		return dropDestDbs;
	}

	public void setDropDestDbs(boolean dropDestDbs) {
		this.dropDestDbs = dropDestDbs;
	}

	public boolean isDropDestDbsAndConfigMetadata() {
		return dropDestDbsAndConfigMetadata;
	}

	public void setDropDestDbsAndConfigMetadata(boolean dropDestDbsAndConfigMetadata) {
		this.dropDestDbsAndConfigMetadata = dropDestDbsAndConfigMetadata;
	}

	public boolean isNonPrivilegedMode() {
		return nonPrivilegedMode;
	}
	
	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
				includeDatabasesAll.add(ns.getDatabaseName());
			} else {
				includeDatabases.add(nsStr);
				includeDatabasesAll.add(nsStr);
			}
		}
	}

	public void setNonPrivilegedMode(boolean nonPrivilegedMode) {
		this.nonPrivilegedMode = nonPrivilegedMode;
	}

	public boolean isDoChunkCounts() {
		return doChunkCounts;
	}

	public void setDoChunkCounts(boolean doChunkCounts) {
		this.doChunkCounts = doChunkCounts;
	}

	public boolean isPreserveUUIDs() {
		return preserveUUIDs;
	}

	public void setPreserveUUIDs(boolean preserveUUIDs) {
		this.preserveUUIDs = preserveUUIDs;
	}

	public String getCompressors() {
		return compressors;
	}

	public void setCompressors(String compressors) {
		this.compressors = compressors;
	}

	public String getOplogBasePath() {
		return oplogBasePath;
	}

	public void setOplogBasePath(String oplogBasePath) {
		this.oplogBasePath = oplogBasePath;
	}

	public String getBookmarkFilePrefix() {
		return bookmarkFilePrefix;
	}

	public void setBookmarkFilePrefix(String bookmarkFilePrefix) {
		this.bookmarkFilePrefix = bookmarkFilePrefix;
	}

	public boolean isReverseSync() {
		return reverseSync;
	}

	public void setReverseSync(boolean reverseSync) {
		this.reverseSync = reverseSync;
	}

	public boolean isNoIndexRestore() {
		return noIndexRestore;
	}

	public void setNoIndexRestore(boolean noIndexRestore) {
		this.noIndexRestore = noIndexRestore;
	}

	public Integer getCollStatsThreshold() {
		return collStatsThreshold;
	}

	public void setCollStatsThreshold(Integer collStatsThreshold) {
		this.collStatsThreshold = collStatsThreshold;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}

	public boolean isShardToRs() {
		return shardToRs;
	}

	public void setShardToRs(boolean shardToRs) {
		this.shardToRs = shardToRs;
	}

	public boolean isExtendTtl() {
		return extendTtl;
	}

	public void setExtendTtl(boolean extendTtl) {
		this.extendTtl = extendTtl;
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

	public File getMongomirrorBinary() {
		return mongomirrorBinary;
	}

	public long getSleepMillis() {
		return sleepMillis;
	}

	public void setSleepMillis(long sleepMillis) {
		this.sleepMillis = sleepMillis;
	}
	
	public void setSleepMillis(String optionValue) {
		if (optionValue != null) {
			this.sleepMillis = Long.parseLong(optionValue);
		}
	}

	public String getNumParallelCollections() {
		return numParallelCollections;
	}

	public void setNumParallelCollections(String numParallelCollections) {
		this.numParallelCollections = numParallelCollections;
	}

	public int getMongoMirrorStartPort() {
		return mongoMirrorStartPort;
	}

	public void setMongoMirrorStartPort(int mongoMirrorStartPort) {
		this.mongoMirrorStartPort = mongoMirrorStartPort;
	}

	public String getWriteConcern() {
		return writeConcern;
	}

	public void setWriteConcern(String writeConcern) {
		this.writeConcern = writeConcern;
	}

	public Long getCleanupOrphansSleepMillis() {
		return cleanupOrphansSleepMillis;
	}
	
	public void setCleanupOrphansSleepMillis(String sleepMillisString) {
		if (sleepMillisString != null) {
			this.cleanupOrphansSleepMillis = Long.parseLong(sleepMillisString);
		}
	}

	public String getDestVersion() {
		return destVersion;
	}

	public void setDestVersion(String destVersion) {
		this.destVersion = destVersion;
	}

	public List<Integer> getDestVersionArray() {
		return destVersionArray;
	}

	public void setDestVersionArray(List<Integer> destVersionArray) {
		this.destVersionArray = destVersionArray;
	}

	public boolean isSslAllowInvalidHostnames() {
		return sslAllowInvalidHostnames;
	}

	public void setSslAllowInvalidHostnames(boolean sslAllowInvalidHostnames) {
		this.sslAllowInvalidHostnames = sslAllowInvalidHostnames;
	}

	public boolean isSslAllowInvalidCertificates() {
		return sslAllowInvalidCertificates;
	}

	public void setSslAllowInvalidCertificates(boolean sslAllowInvalidCertificates) {
		this.sslAllowInvalidCertificates = sslAllowInvalidCertificates;
	}

	public boolean isSkipFlushRouterConfig() {
		return skipFlushRouterConfig;
	}

	public void setSkipFlushRouterConfig(boolean skipFlushRouterConfig) {
		this.skipFlushRouterConfig = skipFlushRouterConfig;
	}
	
	public void setMongomirrorBinary(String binaryPath) {
		if (binaryPath != null) {
			this.mongomirrorBinary = new File(binaryPath);
		}
	}

	public ShardClient getDestShardClient() {
		return destShardClient;
	}

	public void setDestShardClient(ShardClient destShardClient) {
		this.destShardClient = destShardClient;
	}

	public ShardClient getSourceShardClient() {
		return sourceShardClient;
	}

	public void setSourceShardClient(ShardClient sourceShardClient) {
		this.sourceShardClient = sourceShardClient;
	}
}