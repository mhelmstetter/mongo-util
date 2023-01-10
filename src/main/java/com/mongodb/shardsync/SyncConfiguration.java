package com.mongodb.shardsync;

import java.io.File;
import java.util.List;

public class SyncConfiguration extends BaseConfiguration {
	
	public String atlasApiPublicKey;
	public String atlasApiPrivateKey;
	public String atlasProjectId;
	
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
	public boolean extendTtl;
	public File mongomirrorBinary;

	/* Mongomirror email report specific settings */
	public List<String> emailReportRecipients;
	public String smtpHost;
	public int smtpPort;
	public boolean smtpStartTlsEnable;
	public boolean smtpAuth;
	public String smtpPassword;
	public String mailFrom;
	public int errorMessageWindowSecs;
	public int errorReportMax;
	public int emailReportMax;
	public int stopWhenLagWithin;

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
		super();

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

	public boolean isExtendTtl() {
		return extendTtl;
	}

	public void setExtendTtl(boolean extendTtl) {
		this.extendTtl = extendTtl;
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

	public List<String> getEmailReportRecipients() {
		return emailReportRecipients;
	}

	public void setEmailReportRecipients(List<String> emailReportRecipients) {
		this.emailReportRecipients = emailReportRecipients;
	}

	public String getSmtpHost() {
		return smtpHost;
	}

	public void setSmtpHost(String smtpHost) {
		this.smtpHost = smtpHost;
	}

	public int getSmtpPort() {
		return smtpPort;
	}

	public void setSmtpPort(int smtpPort) {
		this.smtpPort = smtpPort;
	}

	public boolean isSmtpStartTlsEnable() {
		return smtpStartTlsEnable;
	}

	public void setSmtpStartTlsEnable(boolean smtpStartTlsEnable) {
		this.smtpStartTlsEnable = smtpStartTlsEnable;
	}

	public boolean isSmtpAuth() {
		return smtpAuth;
	}

	public void setSmtpAuth(boolean smtpAuth) {
		this.smtpAuth = smtpAuth;
	}

	public String getSmtpPassword() {
		return smtpPassword;
	}

	public void setSmtpPassword(String smtpPassword) {
		this.smtpPassword = smtpPassword;
	}

	public void setMailFrom(String mailFrom) {
		this.mailFrom = mailFrom;
	}

	public String getMailFrom() {
		return mailFrom;
	}

	public int getErrorMessageWindowSecs() {
		return errorMessageWindowSecs;
	}

	public void setErrorMessageWindowSecs(int errorMessageWindowSecs) {
		this.errorMessageWindowSecs = errorMessageWindowSecs;
	}

	public int getErrorReportMax() {
		return errorReportMax;
	}

	public void setErrorReportMax(int errorReportMax) {
		this.errorReportMax = errorReportMax;
	}

	public int getEmailReportMax() {
		return emailReportMax;
	}

	public void setEmailReportMax(int emailReportMax) {
		this.emailReportMax = emailReportMax;
	}

	public int getStopWhenLagWithin() {
		return stopWhenLagWithin;
	}

	public void setStopWhenLagWithin(int stopWhenLagWithin) {
		this.stopWhenLagWithin = stopWhenLagWithin;
	}
}