package com.mongodb.model;

import java.util.HashMap;
import java.util.Map;

public class Database {
	
	private String name;
	
	private DatabaseStats dbStats;
	
	private Map<String, Collection> collections;
	private Map<String, Namespace> namespaces;

	public Database(String name, DatabaseStats dbStats) {
		this.name = name;
		this.dbStats = dbStats;
	}

	public void addCollection(Collection coll) {
		if (collections == null) {
			collections = new HashMap<>();
			namespaces = new HashMap<>();
		}
		collections.put(coll.getName(), coll);
		Namespace ns = new Namespace(name, coll.getName());
		namespaces.put(ns.getNamespace(), ns);
	}

	public String getName() {
		return name;
	}

	public DatabaseStats getDbStats() {
		return dbStats;
	}

	public java.util.Collection<Namespace> getNamespaces() {
		return namespaces.values();
	}

}
