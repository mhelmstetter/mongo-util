package com.mongodb.util.bson;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;

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
	
	public static BsonBinary uuidToBsonBinary(UUID uuid) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(uuid.getMostSignificantBits());
        byteBuffer.putLong(uuid.getLeastSignificantBits());
        return new BsonBinary(BsonBinarySubType.UUID_STANDARD, byteBuffer.array());
    }
	
	public static long hashString(String input) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] hashBytes = md5.digest(input.getBytes(StandardCharsets.UTF_8));

            // Take the first 8 bytes of the MD5 hash
            byte[] truncatedHash = Arrays.copyOf(hashBytes, Long.BYTES);

            // Convert byte array to long
            long result = 0;
            for (int i = 0; i < Long.BYTES; ++i) {
                result <<= 8;
                result |= (truncatedHash[i] & 0xFF);
            }

            return result;
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
