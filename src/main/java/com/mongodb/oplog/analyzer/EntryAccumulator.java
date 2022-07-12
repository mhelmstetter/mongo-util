package com.mongodb.oplog.analyzer;

public class EntryAccumulator {
    
    private OplogEntryKey key;
    
    private long count;
    private long total;
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    
    
    
    
    public EntryAccumulator(OplogEntryKey key) {
        this.key = key;
    }

    public void addExecution(long amt) {
        count++;
        total += amt;
        if (amt > max) {
            max = amt;
        }
        if (amt < min) {
            min = amt;
        }
    }
    
    public String toString() {
        return String.format("%-80s %5s %10d %10d %10d %10d %20d", key.ns, key.op, count, min, max, total/count, total);
        
    }

    public long getCount() {
        return count;
    }
    
    public long getMin() {
        return min;
    }
    
    public long getMax() {
        return max;
    }

    public long getAvg() {
        return total/count;
    }
    
    public long getTotal() {
        return total;
    }
    
    public String getNamespace() {
    	return key.ns;
    }
    
    public String getOp() {
    	return key.op;
    }
    
    public String getDate() {
    	return key.date;
    }



}
