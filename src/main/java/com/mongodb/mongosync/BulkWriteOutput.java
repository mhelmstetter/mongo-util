package com.mongodb.mongosync;

import com.mongodb.bulk.BulkWriteResult;

public class BulkWriteOutput {
    //private final BulkWriteResult result;
    private int duplicateKeyExceptionCount = 0;
    private int deletedCount = 0;
    private int modifiedCount = 0;
    private int insertedCount = 0;
    private int upsertedCount = 0;
    
    public BulkWriteOutput() {
    	
    }

//    public BulkWriteOutput(BulkWriteResult bulkWriteResult) {
//        this.result = bulkWriteResult;
//        this.deletedCount = result.getDeletedCount();
//        this.modifiedCount = result.getModifiedCount();
//        this.insertedCount = result.getInsertedCount();
//        this.upsertedCount = result.getUpserts().size();
//        this.duplicateKeyExceptionCount = 0;
//    }

//    public BulkWriteOutput(int deletedCount, int modifiedCount, int insertedCount, int upsertedCount, int duplicateKeyExceptionCount, List<WriteModel<BsonDocument>> failedOps) {
//        this.failedOps = failedOps;
//        this.deletedCount = deletedCount;
//        this.modifiedCount = modifiedCount;
//        this.insertedCount = insertedCount;
//        this.upsertedCount = upsertedCount;
//        this.duplicateKeyExceptionCount = duplicateKeyExceptionCount;
//    }
    
    public BulkWriteOutput(int deletedCount, int modifiedCount, int insertedCount, int upsertedCount, int duplicateKeyExceptionCount) {
        this.deletedCount = deletedCount;
        this.modifiedCount = modifiedCount;
        this.insertedCount = insertedCount;
        this.upsertedCount = upsertedCount;
        this.duplicateKeyExceptionCount = duplicateKeyExceptionCount;
    }
    
    public void incDeleted(int d) {
    	this.deletedCount += d;
    }
    
    public void incModified(int m) {
    	this.modifiedCount += m;
    }
    
    public void incInserted(int i) {
    	this.insertedCount += i;
    }
    
    public void incUpserted(int u) {
    	this.upsertedCount += u;
    }
    
    public void incDuplicateKeyExceptionCount(int d) {
    	this.duplicateKeyExceptionCount += d;
    }

    public int getSuccessfulWritesCount() {
        return deletedCount + modifiedCount + insertedCount + upsertedCount + duplicateKeyExceptionCount;
    }

    @Override
    public String toString() {
        return String.format("deletedCount: %d, modifiedCount: %d, insertedCount: %d, upsertedCount: %d, duplicateKeyExceptionCount: %d",
                deletedCount, modifiedCount, insertedCount, upsertedCount, duplicateKeyExceptionCount);
    }

	public int getDuplicateKeyExceptionCount() {
		return duplicateKeyExceptionCount;
	}

	public int getDeletedCount() {
		return deletedCount;
	}

	public int getModifiedCount() {
		return modifiedCount;
	}

	public int getInsertedCount() {
		return insertedCount;
	}

	public int getUpsertedCount() {
		return upsertedCount;
	}

	public void increment(BulkWriteResult result) {
		incDeleted(result.getDeletedCount());
		incModified(result.getModifiedCount());
		incInserted(result.getInsertedCount());
		incUpserted(result.getUpserts().size());
	}
	
	public void increment(BulkWriteResult result, int errorCount) {
		increment(result);
		incDuplicateKeyExceptionCount(errorCount);
	}
}


