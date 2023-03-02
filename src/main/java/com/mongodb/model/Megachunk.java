package com.mongodb.model;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

public class Megachunk {
	
	private String chunkId;
	private String ns = null;
	private String shard = null;
	private BsonDocument min = null;
	private List<BsonDocument> mids = new LinkedList<>();
	private BsonDocument max = null;
	private boolean isLast = false;

	public boolean isLast() {
		return isLast;
	}

	public void setLast(boolean isLast) {
		this.isLast = isLast;
	}

	public BsonDocument getMin() {
		return min;
	}

	public void setMin(BsonDocument min) {
		this.min = min;
	}

	public List<BsonDocument> getMids() {
		return mids;
	}

	public void setMids(List<BsonDocument> mids) {
		this.mids = mids;
	}

	public BsonDocument getMax() {
		return max;
	}

	public void addMax(BsonDocument max) {
		if (this.max != null) {
			this.mids.add(this.max);
		}
		this.max = max;
	}

	public String getNs() {
		return ns;
	}

	public void setNs(String ns) {
		this.ns = ns;
	}

	public String getShard() {
		return shard;
	}

	public void setShard(String shard) {
		this.shard = shard;
	}

	public String getId() {
		String minHash = ((RawBsonDocument) min).toJson();
		String maxHash = ((RawBsonDocument) max).toJson();
		return String.format("%s_%s_%s", ns, minHash, maxHash);
	}

	public String getChunkId() {
		return chunkId;
	}

	public void setChunkId(String chunkId) {
		this.chunkId = chunkId;
	}

	@Override
	public int hashCode() {
		return Objects.hash(max, min, ns, shard);
		//return Objects.hash(chunkId, shard);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Megachunk other = (Megachunk) obj;
		return Objects.equals(max, other.max) && Objects.equals(min, other.min) && Objects.equals(ns, other.ns)
				&& Objects.equals(shard, other.shard);
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Megachunk [chunkId=");
		builder.append(chunkId);
		builder.append(", ns=");
		builder.append(ns);
		builder.append(", shard=");
		builder.append(shard);
		builder.append(", min=");
		builder.append(min);
		builder.append(", max=");
		builder.append(max);
		builder.append("]");
		return builder.toString();
	}
	
}