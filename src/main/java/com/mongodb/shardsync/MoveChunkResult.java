package com.mongodb.shardsync;

import com.mongodb.MongoCommandException;

/**
 * Result of a chunk move operation with detailed status information.
 */
public class MoveChunkResult {

    private final boolean success;
    private final String errorMessage;
    private final Integer errorCode;
    private final MongoCommandException exception;

    private MoveChunkResult(boolean success, String errorMessage, Integer errorCode, MongoCommandException exception) {
        this.success = success;
        this.errorMessage = errorMessage;
        this.errorCode = errorCode;
        this.exception = exception;
    }

    /**
     * Create a successful result.
     */
    public static MoveChunkResult success() {
        return new MoveChunkResult(true, null, null, null);
    }

    /**
     * Create a failure result from a MongoCommandException.
     */
    public static MoveChunkResult failure(MongoCommandException exception) {
        return new MoveChunkResult(false, exception.getErrorMessage(), exception.getErrorCode(), exception);
    }

    /**
     * Create a failure result with a custom error message.
     */
    public static MoveChunkResult failure(String errorMessage) {
        return new MoveChunkResult(false, errorMessage, null, null);
    }

    public boolean isSuccess() {
        return success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Integer getErrorCode() {
        return errorCode;
    }

    public MongoCommandException getException() {
        return exception;
    }

    /**
     * Get a human-readable description of the result.
     */
    public String getDescription() {
        if (success) {
            return "Move completed successfully";
        } else if (errorMessage != null) {
            if (errorCode != null) {
                return String.format("%s (code: %d)", errorMessage, errorCode);
            }
            return errorMessage;
        } else {
            return "Move failed (no details available)";
        }
    }

    @Override
    public String toString() {
        return getDescription();
    }
}
