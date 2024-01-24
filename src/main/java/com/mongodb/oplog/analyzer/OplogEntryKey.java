package com.mongodb.oplog.analyzer;

import java.util.Objects;

public class OplogEntryKey {
    
    public String ns;
    public String op;
    public String date;
    
    public OplogEntryKey(String ns, String op, String date) {
        this.ns = ns;
        this.op = op;
        this.date = date;
    }
    
    public OplogEntryKey(String ns, String op) {
        this(ns, op, null);
    }

	@Override
	public int hashCode() {
		return Objects.hash(date, ns, op);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OplogEntryKey other = (OplogEntryKey) obj;
		return Objects.equals(date, other.date) && Objects.equals(ns, other.ns) && Objects.equals(op, other.op);
	}


    
    

}
