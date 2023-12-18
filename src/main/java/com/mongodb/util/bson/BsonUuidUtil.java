package com.mongodb.util.bson;

import java.nio.ByteBuffer;
import java.util.UUID;

import org.bson.BsonBinary;

public class BsonUuidUtil {
	
	public static UUID convertBsonBinaryToUuid(BsonBinary bsonBinary) {
        // Check if the BsonBinary is not null and has the correct length
        if (bsonBinary != null) {
            // Extract the byte array from BsonBinary
            byte[] byteArray = bsonBinary.getData();

            // Convert the byte array to a ByteBuffer and then to a UUID
            ByteBuffer buffer = ByteBuffer.wrap(byteArray);
            long mostSignificantBits = buffer.getLong();
            long leastSignificantBits = buffer.getLong();

            return new UUID(mostSignificantBits, leastSignificantBits);
        } else {
            // Handle the case where the BsonBinary is null or has an incorrect length
            throw new IllegalArgumentException("Invalid BsonBinary value for UUID conversion");
        }
    }

}
