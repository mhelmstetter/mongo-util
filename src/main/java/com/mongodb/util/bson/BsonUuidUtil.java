package com.mongodb.util.bson;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;

public class BsonUuidUtil {
	
	public static UUID convertBsonBinaryToUuid(BsonBinary bsonBinary) {
        // Check if the BsonBinary is not null and has the correct length
        if (bsonBinary != null) {
            // Use MongoDB driver's built-in UUID conversion which handles all representations correctly
            byte subtype = bsonBinary.getType();
            
            if (subtype == BsonBinarySubType.UUID_LEGACY.getValue()) {
                // For legacy UUID (subtype 3), we need to determine which legacy format
                // The MongoDB driver supports multiple legacy formats (Java, C#, Python)
                // We'll use C# legacy as it's the most common for cross-platform scenarios
                // If you need Java legacy, change to UuidRepresentation.JAVA_LEGACY
                return bsonBinary.asUuid(UuidRepresentation.C_SHARP_LEGACY);
            } else if (subtype == BsonBinarySubType.UUID_STANDARD.getValue()) {
                // For standard UUID (subtype 4), use the standard representation
                return bsonBinary.asUuid(UuidRepresentation.STANDARD);
            } else {
                // For other subtypes, try to use standard representation
                // This will throw an exception if the binary is not a valid UUID
                return bsonBinary.asUuid();
            }
        } else {
            // Handle the case where the BsonBinary is null
            throw new IllegalArgumentException("Invalid BsonBinary value for UUID conversion");
        }
    }
	
	public static BsonBinary uuidToBsonBinary(UUID uuid) {
        // Use MongoDB driver's built-in conversion to standard UUID (subtype 4)
        return new BsonBinary(uuid);
    }
	
	public static BsonBinary uuidToBsonBinaryLegacy(UUID uuid) {
        // Convert UUID to legacy format (subtype 3) using C# legacy representation
        // If you need Java legacy, change to UuidRepresentation.JAVA_LEGACY
        return new BsonBinary(uuid, UuidRepresentation.C_SHARP_LEGACY);
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
