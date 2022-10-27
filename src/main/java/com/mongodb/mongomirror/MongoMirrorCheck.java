package com.mongodb.mongomirror;

import com.mongodb.shardsync.ShardConfigSyncApp;

import java.io.File;
import java.io.IOException;

/* Temporary launch point for local testing; delete before merging to master */
public class MongoMirrorCheck {


    public static void main(String[] args) {
        String[] scsaArgs = new String[]{"-c", "ss.props", "--mongomirror"};
        try {
            ShardConfigSyncApp.main(scsaArgs);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
