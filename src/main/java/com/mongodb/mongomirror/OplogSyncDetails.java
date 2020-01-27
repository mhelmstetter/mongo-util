package com.mongodb.mongomirror;

import java.time.Duration;

import com.mongodb.util.BsonUtils;

public class OplogSyncDetails {

    private Long currentTimestamp;
    private Long latestTimestamp;
    
    public Duration getLag() {
        long currentEpoch = BsonUtils.getEpochFromBsonTimestamp(currentTimestamp);
        long latestEpoch = BsonUtils.getEpochFromBsonTimestamp(latestTimestamp);
        long lagSeconds = latestEpoch - currentEpoch;
        return Duration.ofSeconds(lagSeconds);
    }

    public Long getCurrentTimestamp() {
        return currentTimestamp;
    }

    public void setCurrentTimestamp(Long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }

    public Long getLatestTimestamp() {
        return latestTimestamp;
    }

    public void setLatestTimestamp(Long latestTimestamp) {
        this.latestTimestamp = latestTimestamp;
    }

}
