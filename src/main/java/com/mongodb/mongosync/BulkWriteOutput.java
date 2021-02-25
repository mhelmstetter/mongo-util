package com.mongodb.mongosync;

import java.util.Collections;
import java.util.List;

import org.bson.BsonDocument;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.WriteModel;

public class BulkWriteOutput {
    private final BulkWriteResult result;
    private final int duplicateKeyExceptionCount;
    private final int deletedCount;
    private final int modifiedCount;
    private final int insertedCount;
    private final int upsertedCount;
    private List<WriteModel<BsonDocument>> failedOps = Collections.emptyList();

    public BulkWriteOutput(BulkWriteResult bulkWriteResult) {
        this.result = bulkWriteResult;
        this.deletedCount = result.getDeletedCount();
        this.modifiedCount = result.getModifiedCount();
        this.insertedCount = result.getInsertedCount();
        this.upsertedCount = result.getUpserts().size();
        this.duplicateKeyExceptionCount = 0;
    }

    public BulkWriteOutput(int deletedCount, int modifiedCount, int insertedCount, int upsertedCount, int duplicateKeyExceptionCount, List<WriteModel<BsonDocument>> failedOps) {
        this.failedOps = failedOps;
        this.result = null;
        this.deletedCount = deletedCount;
        this.modifiedCount = modifiedCount;
        this.insertedCount = insertedCount;
        this.upsertedCount = upsertedCount;
        this.duplicateKeyExceptionCount = duplicateKeyExceptionCount;
    }
    
    public BulkWriteOutput(int deletedCount, int modifiedCount, int insertedCount, int upsertedCount, int duplicateKeyExceptionCount) {
        this.result = null;
        this.deletedCount = deletedCount;
        this.modifiedCount = modifiedCount;
        this.insertedCount = insertedCount;
        this.upsertedCount = upsertedCount;
        this.duplicateKeyExceptionCount = duplicateKeyExceptionCount;
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

	public List<WriteModel<BsonDocument>> getFailedOps() {
		return failedOps;
	}
}


