package com.mongodb.diff3.partition;

import com.mongodb.client.model.Filters;
import com.mongodb.diff3.ChunkDef;
import com.mongodb.model.Namespace;
import org.bson.BsonDocument;
import org.bson.BsonMaxKey;
import org.bson.BsonMinKey;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Partition {
    private final Namespace namespace;
    private final Object lowerBound;
    private final Object upperBound;
    private final long estimatedDocCount;
    private static final BsonMinKey MIN_KEY = new BsonMinKey();
    private static final BsonMaxKey MAX_KEY = new BsonMaxKey();

    public Partition(Namespace namespace, Object lowerBound, Object upperBound, long estimatedDocCount) {
        this.namespace = namespace;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.estimatedDocCount = estimatedDocCount;
    }

    public ChunkDef toChunkDef() {
        return new ChunkDef(namespace, (BsonDocument) lowerBound, (BsonDocument) upperBound);
    }

    public Namespace getNamespace() {
        return namespace;
    }

    public Object getLowerBound() {
        return lowerBound;
    }

    public Object getUpperBound() {
        return upperBound;
    }

    public long getEstimatedDocCount() {
        return estimatedDocCount;
    }

    public Bson query() {
        List<Bson> filters = new ArrayList<>();
        if (!lowerBound.equals(MIN_KEY)) {
            filters.add(Filters.gte("_id", lowerBound));
        }
        if (!upperBound.equals(MAX_KEY)) {
            filters.add(Filters.lt("_id", upperBound));
        }
        switch (filters.size()) {
            case 2:
                return Filters.and(filters);
            case 1:
                return filters.get(0);
            case 0:
                return new BsonDocument();
            default:
                throw new RuntimeException("Unexpected filter size: " + filters.size());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Partition partition = (Partition) o;
        return Objects.equals(namespace, partition.namespace) &&
                Objects.equals(lowerBound, partition.lowerBound) &&
                Objects.equals(upperBound, partition.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(namespace, lowerBound, upperBound);
    }

    @Override
    public String toString() {
        return "ns=" +
                namespace.getNamespace() +
                ", bounds=[" +
                lowerBound.toString() +
                ", " +
                upperBound.toString() +
                "]";
    }
}
