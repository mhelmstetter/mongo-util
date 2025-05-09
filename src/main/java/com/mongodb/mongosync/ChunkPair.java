package com.mongodb.mongosync;

import com.mongodb.shardbalancer.CountingMegachunk;

public class ChunkPair {
	final CountingMegachunk chunk1;
	final CountingMegachunk chunk2;
	
	public ChunkPair(CountingMegachunk chunk1, CountingMegachunk chunk2) {
		this.chunk1 = chunk1;
		this.chunk2 = chunk2;
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ChunkPair chunkPair = (ChunkPair) o;
		return (chunk1.equals(chunkPair.chunk1) && chunk2.equals(chunkPair.chunk2)) ||
			   (chunk1.equals(chunkPair.chunk2) && chunk2.equals(chunkPair.chunk1));
	}
	
	@Override
	public int hashCode() {
		// Order-independent hash code
		return chunk1.hashCode() + chunk2.hashCode();
	}
}
