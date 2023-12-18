package com.mongodb.shardbalancer;

import com.mongodb.model.Megachunk;

public class CountingMegachunk  extends Megachunk {
	
	public CountingMegachunk() {
		super();
	}
	
	public CountingMegachunk(Megachunk megachunk) {
		super();
		this.setChunkId(megachunk.getChunkId());
		this.setMin(megachunk.getMin());
		this.setNs(megachunk.getNs());
		this.addMax(megachunk.getMax());
		this.setShard(megachunk.getShard());
	}
	
	private long seenCount = 0L;
	
	private int uberId;
	
	public void incrementSeenCount() {
		seenCount++;
	}

	public long getSeenCount() {
		return seenCount;
	}

	public void setSeenCount(long seenCount) {
		this.seenCount = seenCount;
	}

	public int getUberId() {
		return uberId;
	}

	public void setUberId(int uberId) {
		this.uberId = uberId;
	}
	
	

}
