package com.mongodb.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Namespace {

    private String databaseName;
    private String collectionName;

    private final static Pattern namespacePattern = Pattern.compile("^(.*?)\\.(.*)$");

    public Namespace(String ns) {
        Matcher m = namespacePattern.matcher(ns);
        if (m.find()) {
            databaseName = m.group(1);
            collectionName = m.group(2);
        }
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public String toString() {
        return databaseName + "." + collectionName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((collectionName == null) ? 0 : collectionName.hashCode());
        result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Namespace other = (Namespace) obj;
        if (collectionName == null) {
            if (other.collectionName != null)
                return false;
        } else if (!collectionName.equals(other.collectionName))
            return false;
        if (databaseName == null) {
            if (other.databaseName != null)
                return false;
        } else if (!databaseName.equals(other.databaseName))
            return false;
        return true;
    }

}
