package com.mongodb.mongomirror;

public interface MongoMirrorEventListener {
    void procFailed(Exception e);
    void procLoggedError(String msg);
    void procLoggedComplete(String msg);
}
