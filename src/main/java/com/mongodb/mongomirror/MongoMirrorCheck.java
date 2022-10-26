package com.mongodb.mongomirror;

import java.io.File;
import java.io.IOException;

/* Temporary launch point for local testing; delete before merging to master */
public class MongoMirrorCheck {


    public static void main(String[] args) {

        String SRC_HOST = "atlas-agi1p1-shard-0/ac-x96bfmt-shard-00-00.lgefwkw.mongodb.net:27017,ac-x96bfmt-shard-00-01.lgefwkw.mongodb.net:27017,ac-x96bfmt-shard-00-02.lgefwkw.mongodb.net:27017";
        String DST_HOST = "atlas-k5kxfx-shard-0/cluster1-shard-00-00.xmcdw.mongodb.net:27017,cluster1-shard-00-01.xmcdw.mongodb.net:27017,cluster1-shard-00-02.xmcdw.mongodb.net:27017";
        MongoMirrorRunner mmr = new MongoMirrorRunner("123");
        mmr.setSourceHost(SRC_HOST);
        mmr.setDestinationHost(DST_HOST);
        mmr.setMongomirrorBinary(new File(String.format("%s/build/mongomirror", System.getProperty("user.dir"))));
        mmr.setSourceUsername("matt");
        mmr.setSourcePassword("tc1234");
        mmr.setSourceAuthenticationDatabase("admin");
        mmr.setSourceSsl(true);
        mmr.setDestinationUsername("matt");
        mmr.setDestinationPassword("tc1234");
        mmr.setDestinationAuthenticationDatabase("admin");
        mmr.setDestinationNoSSL(false);
        mmr.setStopWhenLagWithin(1);
        mmr.addEmailRecipient("matt.holford@gmail.com");
//        mmr.addEmailRecipient("mark.helmstetter@mongodb.com");
        try {
            mmr.execute(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
