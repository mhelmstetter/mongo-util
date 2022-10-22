package com.mongodb.mongomirror;

public interface MMEventListener {
    void procFailed(Exception e);
    void procLoggedError(String msg);
    void procLoggedComplete(String msg);
}
