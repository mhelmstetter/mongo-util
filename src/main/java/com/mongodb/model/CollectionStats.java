package com.mongodb.model;

import org.bson.Document;

public class CollectionStats {
    private boolean sharded;
    private String namespace;
    private long size;
    private long count;
    private Number numIndexes;
    private Number storageSize;
    private Number freeStorageSize;
    private Number totalIndexSize;
    private Number totalSize;

    public static CollectionStats fromDocument(Document doc) {
        CollectionStats stats = new CollectionStats();
        Document wt = (Document)doc.get("wiredTiger");
        Document blockManager = (Document)wt.get("block-manager");
        Number reuse = getNumber(blockManager, "file bytes available for reuse");
        stats.numIndexes = getNumber(doc, "nindexes");
        stats.freeStorageSize = getNumber(doc, "freeStorageSize");
        stats.storageSize = getNumber(doc, "storageSize");
        stats.totalIndexSize = getNumber(doc, "totalIndexSize");
        stats.totalSize = getNumber(doc, "totalSize");
        
        
        if(! reuse.equals(stats.freeStorageSize)) {
        	System.out.println(doc.get("ns") + " reuse differs from freeStorageSize");
        }
        stats.sharded = getBoolean(doc, "sharded");
        stats.namespace = getString(doc, "ns");
        stats.size = getLong(doc, "size");
        stats.count = getLong(doc, "count");
        return stats;
    }
    
    private static Number getNumber(Document doc, String key) {
    	return (Number)doc.get(key);
    }

    private static Boolean getBoolean(Document doc, String key) {
        Object val = doc.get(key);
        if (val != null) {
            if (val instanceof Boolean) {
                return ((Boolean) val).booleanValue();
            } else {
                throw new IllegalArgumentException(String.format("Unexpected type %s, expected Boolean", val
                        .getClass().getName()));
            }
        }
        return false;
    }

    private static String getString(Document doc, String key) {
        Object val = doc.get(key);
        if (val != null) {
            if (val instanceof String) {
                return (String) val;
            } else {
                throw new IllegalArgumentException(String.format("Unexpected type %s, expected String",
                        val.getClass().getName()));
            }
        }
        return null;
    }

    private static Long getLong(Document doc, String key) {
        Object val = doc.get(key);
        if (val != null) {
            if (val instanceof Number) {
                return ((Number)val).longValue();
            } else {
                throw new IllegalArgumentException(String.format("Unexpected type %s, expected Number", val.getClass().getName()));
            }
        }
        return null;
    }

    public boolean isSharded() {
        return sharded;
    }

    public String getNamespace() {
        return namespace;
    }

    public long getSize() {
        return size;
    }

    public long getCount() {
        return count;
    }

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("CollectionStats [namespace=");
		builder.append(namespace);
		builder.append(", size=");
		builder.append(size);
		builder.append(", count=");
		builder.append(count);
		builder.append("]");
		return builder.toString();
	}

	public Number getNumIndexes() {
		return numIndexes;
	}

	public Number getFreeStorageSize() {
		return freeStorageSize;
	}

	public Number getStorageSize() {
		return storageSize;
	}

	public Number getTotalIndexSize() {
		return totalIndexSize;
	}

	public Number getTotalSize() {
		return totalSize;
	}
}
