package com.mongodb.diff3;

public class RetryStatus {
    private final int attempt;
    private final long prevAttempt;
    private final int maxAttempts;
    private final long nextAttemptThreshold;

    public RetryStatus(int attempt, long prevAttempt, int maxAttempts) {
        this.attempt = attempt;
        this.prevAttempt = prevAttempt;
        this.maxAttempts = maxAttempts;
        nextAttemptThreshold = assignNextThreshold();
    }

    private long assignNextThreshold() {
        long interval = (long) (Math.pow(2, attempt) * 1000);
        return prevAttempt + interval;
    }

    public long getNextAttemptThreshold() {
        return nextAttemptThreshold;
    }

    public boolean canRetry() {
        return System.currentTimeMillis() > nextAttemptThreshold;
    }

    public RetryStatus increment() {
        int newAttempt = attempt + 1;
        if (newAttempt >= maxAttempts) {
            return null;
        }
        return new RetryStatus(attempt + 1, System.currentTimeMillis(), maxAttempts);
    }

    public int getAttempt() {
        return attempt;
    }
}
