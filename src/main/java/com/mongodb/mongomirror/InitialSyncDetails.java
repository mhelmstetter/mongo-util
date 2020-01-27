package com.mongodb.mongomirror;

public class InitialSyncDetails {

    private Boolean complete;
    private Long copiedBytes;
    private Long totalBytes;

    public Boolean getComplete() {
        return complete;
    }

    public void setComplete(Boolean complete) {
        this.complete = complete;
    }

    public Long getCopiedBytes() {
        return copiedBytes;
    }

    public void setCopiedBytes(Long copiedBytes) {
        this.copiedBytes = copiedBytes;
    }

    public Long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(Long totalBytes) {
        this.totalBytes = totalBytes;
    }

}
