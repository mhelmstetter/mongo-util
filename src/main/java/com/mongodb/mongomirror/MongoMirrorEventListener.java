package com.mongodb.mongomirror;

public interface MongoMirrorEventListener {
    void procLoggedError(String msg);
    void procLoggedComplete(String msg);
}
