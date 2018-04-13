package com.mongodb.model;

import java.util.Date;

// { "_id" : "ip-172-31-19-4:27017", 
// "ping" : ISODate("2018-04-12T21:16:05.560Z"), "up" : NumberLong(170), "waiting" : true, "mongoVersion" : "3.4.7" }
public class Mongos {

    private String id;
    private Date ping;
    private Long up;
    private boolean waiting;
    private String mongoVersion;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Date getPing() {
        return ping;
    }

    public void setPing(Date ping) {
        this.ping = ping;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public boolean isWaiting() {
        return waiting;
    }

    public void setWaiting(boolean waiting) {
        this.waiting = waiting;
    }

    public String getMongoVersion() {
        return mongoVersion;
    }

    public void setMongoVersion(String mongoVersion) {
        this.mongoVersion = mongoVersion;
    }

}
