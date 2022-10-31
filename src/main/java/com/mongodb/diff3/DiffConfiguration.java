package com.mongodb.diff3;

import static com.mongodb.client.model.Filters.regex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.bson.Document;

import com.mongodb.model.Namespace;

public class DiffConfiguration {

	public String sourceClusterUri;
	public String destClusterUri;
	private boolean filtered;
	private Document chunkQuery = new Document();
	private int threads = 8;

	private Set<Namespace> includeNamespaces = new HashSet<Namespace>();
	private Set<String> includeDatabases = new HashSet<String>();

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

	public void setNamespaceFilters(String[] namespaceFilterList) {
		if (namespaceFilterList == null) {
			return;
		}
		filtered = true;
		for (String nsStr : namespaceFilterList) {
			if (nsStr.contains(".")) {
				Namespace ns = new Namespace(nsStr);
				includeNamespaces.add(ns);
			} else {
				includeDatabases.add(nsStr);
			}
		}
		initializeChunkQuery();
	}

	private Document initializeChunkQuery() {
		if (getIncludeNamespaces().size() > 0 || getIncludeDatabases().size() > 0) {
			List inList = new ArrayList();
			List orList = new ArrayList();
			// Document orDoc = new Document("$or", orList);
			chunkQuery.append("$or", orList);
			Document inDoc = new Document("ns", new Document("$in", inList));
			orList.add(inDoc);
			// orDoc.append("ns", inDoc);
			for (Namespace includeNs : getIncludeNamespaces()) {
				inList.add(includeNs.getNamespace());
			}
			for (String dbName : getIncludeDatabases()) {
				orList.add(regex("ns", "^" + dbName + "\\."));
			}
		} else {
			chunkQuery.append("ns", new Document("$ne", "config.system.sessions"));
		}
		return chunkQuery;
	}

	public boolean isFiltered() {
		return filtered;
	}

	public Document getChunkQuery() {
		if (chunkQuery.isEmpty()) {
			chunkQuery.append("ns", new Document("$ne", "config.system.sessions"));
		}
		return chunkQuery;
	}

	public int getThreads() {
		return threads;
	}

	public void setThreads(int threads) {
		this.threads = threads;
	}

}
