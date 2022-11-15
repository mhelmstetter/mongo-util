package com.mongodb.diff3;

public class RetryStatus {
    private final int attempt;
    private final long prevAttempt;
    private final int MAX_ATTEMPTS = 5;
    private long nextAttemptThreshold;

    public RetryStatus(int attempt, long prevAttempt) {
        this.attempt = attempt;
        this.prevAttempt = prevAttempt;
        nextAttemptThreshold = assignNextThreshold();
    }

    private long assignNextThreshold() {
        long interval = (long) (Math.pow(2, attempt) * 1000);
        return prevAttempt + interval;
    }

    public boolean canRetry() {
        return System.currentTimeMillis() > nextAttemptThreshold;
    }

    public RetryStatus increment() {
        int newAttempt = attempt + 1;
        if (newAttempt >= MAX_ATTEMPTS) {
            return null;
        }
        return new RetryStatus(attempt + 1, System.currentTimeMillis());
    }

    public int getAttempt() {
        return attempt;
    }
}
