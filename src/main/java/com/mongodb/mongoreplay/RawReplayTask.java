package com.mongodb.mongoreplay;

import java.util.concurrent.Callable;

import org.bson.BSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RawReplayTask implements Callable<ReplayResult> {

    
    protected static final Logger logger = LoggerFactory.getLogger(RawReplayTask.class);
    
    private Replayer replayer;
    private BSONObject raw;

    public RawReplayTask(Replayer replayer, BSONObject raw) {
        this.replayer = replayer;
        this.raw = raw;
    }
   

    @Override
    public ReplayResult call() {
        return replayer.replay(raw);
    }
}
