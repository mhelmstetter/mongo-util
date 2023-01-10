package com.mongodb.diff3;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;

import com.mongodb.diff3.DiffResult.MismatchEntry;
import com.mongodb.diff3.DiffSummary.DiffStatus;

public class ChunkResult {
    private final LongAdder matches;
    private final LongAdder bytesProcessed;
    private final Set<DiffResult.MismatchEntry> mismatches;
    private final Set<BsonValue> sourceOnly;
    private final Set<BsonValue> destOnly;
    private final AtomicInteger retryNum;
    private DiffStatus status;

    ChunkResult() {
        matches = new LongAdder();
        bytesProcessed = new LongAdder();
        mismatches = ConcurrentHashMap.newKeySet();
        sourceOnly = ConcurrentHashMap.newKeySet();
        destOnly = ConcurrentHashMap.newKeySet();
        retryNum = new AtomicInteger(0);
        status = DiffStatus.UNSTARTED;
    }

    public LongAdder getMatches() {
        return matches;
    }

    public void addMatches(long matches) {
        this.matches.add(matches);
    }

    public LongAdder getBytesProcessed() {
        return bytesProcessed;
    }

    public void addBytesProcessed(long bytesProcessed) {
        this.bytesProcessed.add(bytesProcessed);
    }

    public Set<DiffResult.MismatchEntry> getMismatches() {
        return mismatches;
    }

    public Set<BsonDocument> getMismatchDocs() {
        return mismatches.stream().map(m -> {
            BsonDocument d = new BsonDocument();
            d.put("key", m.getKey());
            d.put("srcChksum", new BsonString(m.getSrcChksum()));
            d.put("destChksum", new BsonString(m.getDestChksum()));
            return d;
        }).collect(Collectors.toSet());
    }

    public void setMismatches(Set<DiffResult.MismatchEntry> mismatches) {
        this.mismatches.clear();
        this.mismatches.addAll(mismatches);
    }

    public void removeMismatches() {
        this.mismatches.clear();
    }

    public Set<BsonValue> getSourceOnly() {
        return sourceOnly;
    }

    public void setSourceOnly(Set<BsonValue> sourceOnly) {
        this.sourceOnly.clear();
        this.sourceOnly.addAll(sourceOnly);
    }

    public void removeSourceOnly() {
        this.sourceOnly.clear();
    }

    public Set<BsonValue> getDestOnly() {
        return destOnly;
    }

    public void setDestOnly(Set<BsonValue> destOnly) {
        this.destOnly.clear();
        this.destOnly.addAll(destOnly);
    }

    public void removeDestOnly() {
        this.destOnly.clear();
    }

    public synchronized DiffStatus getStatus() {
        return status;
    }

    public synchronized void setStatus(DiffStatus status) {
        this.status = status;
    }

    public AtomicInteger getRetryNum() {
        return retryNum;
    }
}