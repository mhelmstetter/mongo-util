package com.mongodb.diff3;

import com.mongodb.model.Namespace;
import org.bson.BsonDocument;
import org.bson.Document;

public class ChunkDef {
    private final Namespace ns;
    private final BsonDocument min;
    private final BsonDocument max;

    public ChunkDef(Namespace ns, BsonDocument min, BsonDocument max) {
        this.ns = ns;
        this.min = min;
        this.max = max;
    }

    public Namespace getNs() {
        return ns;
    }

    public BsonDocument getMin() {
        return min;
    }

    public BsonDocument getMax() {
        return max;
    }

    public String unitString() {
        String minRep = min == null ? "ALL" : min.toString();
        String maxRep = max == null ? "ALL" : max.toString();
        return ns.getNamespace() + ": [" + minRep + " -- " + maxRep + "]";
    }
}
