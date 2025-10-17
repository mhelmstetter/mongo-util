package com.mongodb.shardsync;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks the results and errors for each step of the sync metadata process.
 * Provides comprehensive reporting of success/failure status for all operations.
 */
public class SyncMetadataResult {
    
    public enum StepStatus {
        SUCCESS, FAILED, SKIPPED, WARNING
    }
    
    public static class StepResult {
        private final String stepName;
        private final StepStatus status;
        private final String message;
        private final Exception exception;
        
        public StepResult(String stepName, StepStatus status, String message) {
            this(stepName, status, message, null);
        }
        
        public StepResult(String stepName, StepStatus status, String message, Exception exception) {
            this.stepName = stepName;
            this.status = status;
            this.message = message;
            this.exception = exception;
        }
        
        public String getStepName() { return stepName; }
        public StepStatus getStatus() { return status; }
        public String getMessage() { return message; }
        public Exception getException() { return exception; }
        
        public boolean isSuccess() { return status == StepStatus.SUCCESS; }
        public boolean isFailed() { return status == StepStatus.FAILED; }
        public boolean isWarning() { return status == StepStatus.WARNING; }
    }
    
    private final List<StepResult> steps = new ArrayList<>();
    private boolean overallSuccess = true;
    private boolean hasWarnings = false;
    
    public void addStep(String stepName, StepStatus status, String message) {
        addStep(stepName, status, message, null);
    }
    
    public void addStep(String stepName, StepStatus status, String message, Exception exception) {
        StepResult result = new StepResult(stepName, status, message, exception);
        steps.add(result);
        
        if (status == StepStatus.FAILED) {
            overallSuccess = false;
        } else if (status == StepStatus.WARNING) {
            hasWarnings = true;
        }
    }
    
    public void addSuccess(String stepName, String message) {
        addStep(stepName, StepStatus.SUCCESS, message);
    }
    
    public void addFailure(String stepName, String message) {
        addStep(stepName, StepStatus.FAILED, message);
    }
    
    public void addFailure(String stepName, String message, Exception exception) {
        addStep(stepName, StepStatus.FAILED, message, exception);
    }
    
    public void addWarning(String stepName, String message) {
        addStep(stepName, StepStatus.WARNING, message);
    }
    
    public void addSkipped(String stepName, String message) {
        addStep(stepName, StepStatus.SKIPPED, message);
    }
    
    public List<StepResult> getSteps() { return steps; }
    public boolean isOverallSuccess() { return overallSuccess; }
    public boolean hasWarnings() { return hasWarnings; }
    
    public int getSuccessCount() {
        return (int) steps.stream().filter(StepResult::isSuccess).count();
    }
    
    public int getFailureCount() {
        return (int) steps.stream().filter(StepResult::isFailed).count();
    }
    
    public int getWarningCount() {
        return (int) steps.stream().filter(StepResult::isWarning).count();
    }
    
    public List<StepResult> getFailedSteps() {
        return steps.stream().filter(StepResult::isFailed).toList();
    }
    
    public List<StepResult> getWarningSteps() {
        return steps.stream().filter(StepResult::isWarning).toList();
    }

    /**
     * Merges another result into this one, combining all steps and updating
     * overall status flags appropriately.
     *
     * @param other The result to merge into this one
     */
    public void merge(SyncMetadataResult other) {
        this.steps.addAll(other.steps);
        if (!other.overallSuccess) {
            this.overallSuccess = false;
        }
        if (other.hasWarnings) {
            this.hasWarnings = true;
        }
    }
}