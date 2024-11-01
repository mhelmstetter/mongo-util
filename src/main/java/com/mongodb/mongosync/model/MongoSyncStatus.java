package com.mongodb.mongosync.model;

import com.google.gson.annotations.SerializedName;

public class MongoSyncStatus {

    private Progress progress;
    private boolean success;

    // Getters and setters
    public Progress getProgress() {
        return progress;
    }

    public void setProgress(Progress progress) {
        this.progress = progress;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    // Nested class for Progress
    public static class Progress {
        private MongoSyncState state;
        private boolean canCommit;
        private boolean canWrite;
        private String info;
        private int lagTimeSeconds;
        private CollectionCopy collectionCopy;
        private DirectionMapping directionMapping;
        private int totalEventsApplied;
        private String mongosyncID;
        private String coordinatorID;

        // Getters and setters for Progress fields
        public MongoSyncState getState() {
            return state;
        }

        public void setState(MongoSyncState state) {
            this.state = state;
        }

        public boolean isCanCommit() {
            return canCommit;
        }

        public void setCanCommit(boolean canCommit) {
            this.canCommit = canCommit;
        }

        public boolean isCanWrite() {
            return canWrite;
        }

        public void setCanWrite(boolean canWrite) {
            this.canWrite = canWrite;
        }

        public String getInfo() {
            return info;
        }

        public void setInfo(String info) {
            this.info = info;
        }

        public int getLagTimeSeconds() {
            return lagTimeSeconds;
        }

        public void setLagTimeSeconds(int lagTimeSeconds) {
            this.lagTimeSeconds = lagTimeSeconds;
        }

        public CollectionCopy getCollectionCopy() {
            return collectionCopy;
        }

        public void setCollectionCopy(CollectionCopy collectionCopy) {
            this.collectionCopy = collectionCopy;
        }

        public DirectionMapping getDirectionMapping() {
            return directionMapping;
        }

        public void setDirectionMapping(DirectionMapping directionMapping) {
            this.directionMapping = directionMapping;
        }

        public int getTotalEventsApplied() {
            return totalEventsApplied;
        }

        public void setTotalEventsApplied(int totalEventsApplied) {
            this.totalEventsApplied = totalEventsApplied;
        }

        public String getMongosyncID() {
            return mongosyncID;
        }

        public void setMongosyncID(String mongosyncID) {
            this.mongosyncID = mongosyncID;
        }

        public String getCoordinatorID() {
            return coordinatorID;
        }

        public void setCoordinatorID(String coordinatorID) {
            this.coordinatorID = coordinatorID;
        }

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("Progress [");
			if (state != null) {
				builder.append("state=");
				builder.append(state);
				builder.append(", ");
			}
			builder.append("canCommit=");
			builder.append(canCommit);
			builder.append(", canWrite=");
			builder.append(canWrite);
			builder.append(", ");
			if (info != null) {
				builder.append("info=");
				builder.append(info);
				builder.append(", ");
			}
			builder.append("lagTimeSeconds=");
			builder.append(lagTimeSeconds);
			builder.append(", ");
			if (collectionCopy != null) {
				builder.append("collectionCopy=");
				builder.append(collectionCopy);
				builder.append(", ");
			}
			if (directionMapping != null) {
				builder.append("directionMapping=");
				builder.append(directionMapping);
				builder.append(", ");
			}
			builder.append("totalEventsApplied=");
			builder.append(totalEventsApplied);
			builder.append(", ");
			if (mongosyncID != null) {
				builder.append("mongosyncID=");
				builder.append(mongosyncID);
				builder.append(", ");
			}
			if (coordinatorID != null) {
				builder.append("coordinatorID=");
				builder.append(coordinatorID);
			}
			builder.append("]");
			return builder.toString();
		}
    }

    // Nested class for CollectionCopy
    public static class CollectionCopy {
        private long estimatedTotalBytes;
        private long estimatedCopiedBytes;

        // Getters and setters for CollectionCopy fields
        public long getEstimatedTotalBytes() {
            return estimatedTotalBytes;
        }

        public void setEstimatedTotalBytes(long estimatedTotalBytes) {
            this.estimatedTotalBytes = estimatedTotalBytes;
        }

        public long getEstimatedCopiedBytes() {
            return estimatedCopiedBytes;
        }

        public void setEstimatedCopiedBytes(long estimatedCopiedBytes) {
            this.estimatedCopiedBytes = estimatedCopiedBytes;
        }

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("CollectionCopy [estimatedTotalBytes=");
			builder.append(estimatedTotalBytes);
			builder.append(", estimatedCopiedBytes=");
			builder.append(estimatedCopiedBytes);
			builder.append("]");
			return builder.toString();
		}
    }

    // Nested class for DirectionMapping
    public static class DirectionMapping {
        @SerializedName("Source")
        private String source;
        @SerializedName("Destination")
        private String destination;

        // Getters and setters for DirectionMapping fields
        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getDestination() {
            return destination;
        }

        public void setDestination(String destination) {
            this.destination = destination;
        }

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();
			builder.append("DirectionMapping [");
			if (source != null) {
				builder.append("source=");
				builder.append(source);
				builder.append(", ");
			}
			if (destination != null) {
				builder.append("destination=");
				builder.append(destination);
			}
			builder.append("]");
			return builder.toString();
		}
    }

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("MongoSyncStatus [progress=");
		builder.append(progress);
		builder.append(", success=");
		builder.append(success);
		builder.append("]");
		return builder.toString();
	}
}

