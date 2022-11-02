package com.mongodb.model;

import org.bson.Document;

public class CollectionStats {
    private boolean sharded;
    private String namespace;
    private long size;
    private long count;

    public static CollectionStats fromDocument(Document doc) {
        CollectionStats stats = new CollectionStats();
        stats.sharded = getBoolean(doc, "sharded");
        stats.namespace = getString(doc, "ns");
        stats.size = getLong(doc, "size");
        stats.count = getLong(doc, "count");
        return stats;
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
        return null;
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
}
