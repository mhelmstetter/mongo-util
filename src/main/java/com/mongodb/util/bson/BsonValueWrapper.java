package com.mongodb.util.bson;

import java.util.Arrays;
import java.util.Objects;

import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

public class BsonValueWrapper implements Comparable<BsonValueWrapper> {

    private final BsonValue value;

    public BsonValueWrapper(BsonValue value) {
        this.value = value;
    }

    @Override
    public int compareTo(BsonValueWrapper other) {
        System.out.println("DEBUG BsonValueWrapper.compareTo: " + this.value + " vs " + other.value);
        
        if (value == null && other.value == null) {
            System.out.println("DEBUG: Both null, returning 0");
            return 0;
        } else if (value == null) {
            System.out.println("DEBUG: This is null, returning -1");
            return -1;
        } else if (other.value == null) {
            System.out.println("DEBUG: Other is null, returning 1");
            return 1;
        }

        if (value.getBsonType() == BsonType.MIN_KEY) {
            if (other.value.getBsonType() == BsonType.MIN_KEY) {
                return 0; // MIN_KEY compared to MIN_KEY is equal
            } else {
                return -1; // MIN_KEY is less than any other value
            }
        } else if (other.value.getBsonType() == BsonType.MIN_KEY) {
            return 1; // Any value other than MIN_KEY is greater than MIN_KEY
        }

        if (value.getBsonType() == BsonType.MAX_KEY) {
            if (other.value.getBsonType() == BsonType.MAX_KEY) {
                return 0; // MAX_KEY compared to MAX_KEY is equal
            } else {
                return 1; // MAX_KEY is greater than any other value
            }
        } else if (other.value.getBsonType() == BsonType.MAX_KEY) {
            return -1; // Any value other than MAX_KEY is less than MAX_KEY
        }

        if (value.getClass() == other.value.getClass() && value instanceof Comparable<?> && other.value instanceof Comparable<?>) {
            System.out.println("DEBUG: Using direct Comparable comparison");
            @SuppressWarnings("unchecked")
            Comparable<Object> comparableValue = (Comparable<Object>) value;
            @SuppressWarnings("unchecked")
            Comparable<Object> otherComparableValue = (Comparable<Object>) other.value;
            int result = comparableValue.compareTo(otherComparableValue);
            System.out.println("DEBUG: Direct comparison result: " + result);
            return result;
        } else {
            System.out.println("DEBUG: Using compareNonComparableValues");
            int result = compareNonComparableValues(value, other.value);
            System.out.println("DEBUG: compareNonComparableValues result: " + result);
            return result;
        }
    }

    private int compareNonComparableValues(BsonValue value1, BsonValue value2) {
        System.out.println("DEBUG compareNonComparableValues: " + value1 + " vs " + value2);
        System.out.println("DEBUG types: " + value1.getBsonType() + " vs " + value2.getBsonType());
        
        if (value1.isDocument() && value2.isDocument()) {
            System.out.println("DEBUG: Both are documents, using ComparableBsonDocument");
            BsonDocument doc1 = value1.asDocument();
            BsonDocument doc2 = value2.asDocument();
            int result = new ComparableBsonDocument(doc1).compareTo(new ComparableBsonDocument(doc2));
            System.out.println("DEBUG: ComparableBsonDocument result: " + result);
            return result;
        } else if (value1.isArray() && value2.isArray()) {
            return compareArrays(value1.asArray(), value2.asArray());
        } else if (value1.isNumber() && value2.isNumber()) {
            return compareNumbers(value1, value2);
        } else {
            BsonType type1 = value1.getBsonType();
            BsonType type2 = value2.getBsonType();

            if (type1 == type2) {
                switch (type1) {
                    case STRING:
                        return value1.asString().getValue().compareTo(value2.asString().getValue());
                    case BOOLEAN:
                        return Boolean.compare(value1.asBoolean().getValue(), value2.asBoolean().getValue());
                    case DATE_TIME:
                        return Long.compare(value1.asDateTime().getValue(), value2.asDateTime().getValue());
                    case OBJECT_ID:
                        return value1.asObjectId().getValue().compareTo(value2.asObjectId().getValue());
                    case DECIMAL128:
                        return value1.asDecimal128().getValue().compareTo(value2.asDecimal128().getValue());
                    case BINARY:
                    	return compareBsonBinary(value1, value2);
                    case JAVASCRIPT:
                    case JAVASCRIPT_WITH_SCOPE:
                    case REGULAR_EXPRESSION:
                    case SYMBOL:
                    case DB_POINTER:
                    case TIMESTAMP:
                        return value1.asString().getValue().compareTo(value2.asString().getValue());
                    default:
                        throw new IllegalArgumentException("Unsupported BsonType: " + type1);
                }
            } else {
                // Types are different, compare based on their order
                return type1.compareTo(type2);
            }
        }
    }
    
    private int compareBsonBinary(BsonValue value1, BsonValue value2) {
        // Step 1: Compare the type byte
    	BsonBinary b1 = (BsonBinary)value1;
    	BsonBinary b2 = (BsonBinary)value2;
        int typeComparison = Integer.compare(value1.getBsonType().getValue(), value2.getBsonType().getValue());
        if (typeComparison != 0) {
            return typeComparison;
        }

        // Step 2: Compare the data arrays lexicographically
        return Arrays.compare(b1.getData(), b2.getData());
    }
    
    private int compareNumbers(BsonValue value1, BsonValue value2) {
        if (value1.isDouble() || value2.isDouble()) {
            double double1 = value1.isDouble() ? value1.asDouble().getValue() : value1.asNumber().doubleValue();
            double double2 = value2.isDouble() ? value2.asDouble().getValue() : value2.asNumber().doubleValue();
            return Double.compare(double1, double2);
        } else if (value1.isInt64() || value2.isInt64()) {
            long long1 = value1.isInt64() ? value1.asInt64().getValue() : value1.asNumber().longValue();
            long long2 = value2.isInt64() ? value2.asInt64().getValue() : value2.asNumber().longValue();
            return Long.compare(long1, long2);
        } else if (value1.isInt32() && value2.isInt32()) {
            int int1 = value1.asInt32().getValue();
            int int2 = value2.asInt32().getValue();
            return Integer.compare(int1, int2);
        } else {
            Number number1 = value1.asNumber().intValue();
            Number number2 = value2.asNumber().intValue();
            return number1.intValue() - number2.intValue();
        }
    }

    private int compareArrays(BsonArray array1, BsonArray array2) {
        int size1 = array1.size();
        int size2 = array2.size();

        int minSize = Math.min(size1, size2);
        for (int i = 0; i < minSize; i++) {
            BsonValue value1 = array1.get(i);
            BsonValue value2 = array2.get(i);

            int comparison = new BsonValueWrapper(value1).compareTo(new BsonValueWrapper(value2));
            if (comparison != 0) {
                return comparison;
            }
        }

        return Integer.compare(size1, size2);
    }

    public BsonValue getValue() {
        return value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BsonValueWrapper other = (BsonValueWrapper) obj;
        return Objects.equals(value, other.value);
    }

    @Override
    public String toString() {
        return value.toString();
    }
}