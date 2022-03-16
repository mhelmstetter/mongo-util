package com.mongodb.model;

import java.util.Date;

import org.bson.Document;
import org.bson.types.ObjectId;

/*
 * {
    "_id" : "d1.myColl",
    "lastmodEpoch" : ObjectId("5acec0f8edf1355b363d6cec"),
    "lastmod" : ISODate("1970-02-19T17:02:47.296Z"),
    "dropped" : false,
    "key" : {
        "yyy" : 1
    },
    "defaultCollation" : {
        "locale" : "fr",
        "caseLevel" : false,
        "caseFirst" : "off",
        "strength" : 3,
        "numericOrdering" : false,
        "alternate" : "non-ignorable",
        "maxVariable" : "punct",
        "normalization" : false,
        "backwards" : false,
        "version" : "57.1"
    },
    "unique" : false
}
 */
public class ShardCollection {

    private String id;
    private Namespace namespace;
    private ObjectId lastmodEpoch;
    private Date lastmod;
    private boolean dropped;
    private Document key;
    private Document defaultCollation;
    private boolean unique;
    private boolean noBalance;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
        this.namespace = new Namespace(id);
    }
    
    public Namespace getNamespace() {
        return namespace;
    }

    public ObjectId getLastmodEpoch() {
        return lastmodEpoch;
    }

    public void setLastmodEpoch(ObjectId lastmodEpoch) {
        this.lastmodEpoch = lastmodEpoch;
    }

    public Date getLastmod() {
        return lastmod;
    }

    public void setLastmod(Date lastmod) {
        this.lastmod = lastmod;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public Document getKey() {
        return key;
    }

    public void setKey(Document key) {
        this.key = key;
    }

    public Document getDefaultCollation() {
        return defaultCollation;
    }

    public void setDefaultCollation(Document defaultCollation) {
        this.defaultCollation = defaultCollation;
    }

    public boolean isUnique() {
        return unique;
    }

    public void setUnique(boolean unique) {
        this.unique = unique;
    }

    public boolean isNoBalance() {
        return noBalance;
    }

    public void setNoBalance(boolean noBalance) {
        this.noBalance = noBalance;
    }

    @Override
    public String toString() {
        return id;
    }

}
