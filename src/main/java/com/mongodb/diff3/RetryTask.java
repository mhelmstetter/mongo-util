package com.mongodb.diff3;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public interface RetryTask extends Callable<DiffResult>, Delayed {
    RetryStatus getRetryStatus();

    @Override
    default long getDelay(@NotNull TimeUnit unit) {
        if (getRetryStatus() == null) {
            return 0L;
        }
        return unit.convert(getRetryStatus().getNextAttemptThreshold() - System.currentTimeMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    default int compareTo(@NotNull Delayed o) {
        assert o instanceof RetryTask;
        RetryTask ort = (RetryTask) o;
        if (this.getRetryStatus() == null) {
            return -1;
        }
        if (ort.getRetryStatus() == null) {
            return 1;
        }
        if (this.getRetryStatus().getNextAttemptThreshold() > ort.getRetryStatus().getNextAttemptThreshold()) {
            return 1;
        } else if (this.getRetryStatus().getNextAttemptThreshold() < ort.getRetryStatus().getNextAttemptThreshold()) {
            return -1;
        } else {
            return 0;
        }
    }
}
