package com.mongodb.diff3;

public class RetryStatus {
    private final int attempt;
    private final long prevAttempt;
    private final int MAX_ATTEMPTS = 5;

    public RetryStatus(int attempt, long prevAttempt) {
        this.attempt = attempt;
        this.prevAttempt = prevAttempt;
    }

    public boolean canRetry() {
        long threshold = (long) Math.pow(2, attempt) + prevAttempt;
        long now = System.currentTimeMillis();
        return now >= threshold;
    }

    public RetryStatus increment() {
        int newAttempt = attempt + 1;
        if (newAttempt >= MAX_ATTEMPTS) {
            return null;
        }
        return new RetryStatus(attempt + 1, System.currentTimeMillis());
    }
}
