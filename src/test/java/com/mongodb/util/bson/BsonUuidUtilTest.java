package com.mongodb.util.bson;

import org.bson.BsonBinary;
import org.bson.BsonBinarySubType;
import org.bson.UuidRepresentation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BsonUuidUtilTest {

    @Test
    @DisplayName("Test standard UUID (subtype 4) round-trip conversion")
    void testStandardUuidRoundTrip() {
        UUID originalUuid = UUID.randomUUID();
        
        BsonBinary bsonBinary = BsonUuidUtil.uuidToBsonBinary(originalUuid);
        assertEquals(BsonBinarySubType.UUID_STANDARD.getValue(), bsonBinary.getType());
        
        UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
        assertEquals(originalUuid, convertedUuid);
        assertEquals(originalUuid.toString(), convertedUuid.toString());
    }

    @Test
    @DisplayName("Test legacy C# UUID (subtype 3) round-trip conversion")
    void testLegacyCSharpUuidRoundTrip() {
        UUID originalUuid = UUID.randomUUID();
        
        BsonBinary bsonBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(originalUuid);
        assertEquals(BsonBinarySubType.UUID_LEGACY.getValue(), bsonBinary.getType());
        
        UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
        assertEquals(originalUuid, convertedUuid);
        assertEquals(originalUuid.toString(), convertedUuid.toString());
    }

    @Test
    @DisplayName("Test multiple UUIDs with standard representation")
    void testMultipleStandardUuids() {
        UUID[] testUuids = {
            UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
            UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
            UUID.randomUUID(),
            UUID.randomUUID()
        };
        
        for (UUID originalUuid : testUuids) {
            BsonBinary bsonBinary = BsonUuidUtil.uuidToBsonBinary(originalUuid);
            UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
            
            assertEquals(originalUuid, convertedUuid, 
                "UUID mismatch for: " + originalUuid);
            assertEquals(originalUuid.toString(), convertedUuid.toString(),
                "String representation mismatch for: " + originalUuid);
        }
    }

    @Test
    @DisplayName("Test multiple UUIDs with legacy representation")
    void testMultipleLegacyUuids() {
        UUID[] testUuids = {
            UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
            UUID.fromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"),
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8"),
            UUID.randomUUID(),
            UUID.randomUUID()
        };
        
        for (UUID originalUuid : testUuids) {
            BsonBinary bsonBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(originalUuid);
            UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
            
            assertEquals(originalUuid, convertedUuid, 
                "UUID mismatch for: " + originalUuid);
            assertEquals(originalUuid.toString(), convertedUuid.toString(),
                "String representation mismatch for: " + originalUuid);
        }
    }

    @Test
    @DisplayName("Test direct BsonBinary creation with standard UUID representation")
    void testDirectBsonBinaryStandardUuid() {
        UUID originalUuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        
        BsonBinary bsonBinary = new BsonBinary(originalUuid, UuidRepresentation.STANDARD);
        
        UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
        assertEquals(originalUuid, convertedUuid);
        assertEquals(originalUuid.toString(), convertedUuid.toString());
    }

    @Test
    @DisplayName("Test direct BsonBinary creation with C# legacy UUID representation")
    void testDirectBsonBinaryCSharpLegacyUuid() {
        UUID originalUuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        
        BsonBinary bsonBinary = new BsonBinary(originalUuid, UuidRepresentation.C_SHARP_LEGACY);
        
        UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
        assertEquals(originalUuid, convertedUuid);
        assertEquals(originalUuid.toString(), convertedUuid.toString());
    }

    @Test
    @DisplayName("Test direct BsonBinary creation with Java legacy UUID representation")
    void testDirectBsonBinaryJavaLegacyUuid() {
        UUID originalUuid = UUID.fromString("123e4567-e89b-12d3-a456-426614174000");
        
        BsonBinary bsonBinary = new BsonBinary(originalUuid, UuidRepresentation.JAVA_LEGACY);
        
        // Note: Java legacy UUID uses a different byte order, so the conversion
        // through C# legacy format in BsonUuidUtil will not preserve the original UUID
        // This test documents the current behavior where Java legacy UUIDs 
        // are interpreted as C# legacy format, resulting in a different UUID
        UUID convertedUuid = BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary);
        assertNotNull(convertedUuid);
        // The UUIDs won't match due to different byte ordering between Java and C# legacy formats
        assertNotEquals(originalUuid, convertedUuid);
    }

    @Test
    @DisplayName("Test null BsonBinary throws IllegalArgumentException")
    void testNullBsonBinaryThrowsException() {
        assertThrows(IllegalArgumentException.class, 
            () -> BsonUuidUtil.convertBsonBinaryToUuid(null),
            "Should throw IllegalArgumentException for null BsonBinary");
    }

    @Test
    @DisplayName("Test UUID with all zeros")
    void testZeroUuid() {
        UUID zeroUuid = new UUID(0L, 0L);
        
        BsonBinary standardBinary = BsonUuidUtil.uuidToBsonBinary(zeroUuid);
        UUID standardConverted = BsonUuidUtil.convertBsonBinaryToUuid(standardBinary);
        assertEquals(zeroUuid, standardConverted);
        assertEquals("00000000-0000-0000-0000-000000000000", standardConverted.toString());
        
        BsonBinary legacyBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(zeroUuid);
        UUID legacyConverted = BsonUuidUtil.convertBsonBinaryToUuid(legacyBinary);
        assertEquals(zeroUuid, legacyConverted);
        assertEquals("00000000-0000-0000-0000-000000000000", legacyConverted.toString());
    }

    @Test
    @DisplayName("Test UUID with all ones (max value)")
    void testMaxUuid() {
        UUID maxUuid = new UUID(-1L, -1L);
        
        BsonBinary standardBinary = BsonUuidUtil.uuidToBsonBinary(maxUuid);
        UUID standardConverted = BsonUuidUtil.convertBsonBinaryToUuid(standardBinary);
        assertEquals(maxUuid, standardConverted);
        assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", standardConverted.toString());
        
        BsonBinary legacyBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(maxUuid);
        UUID legacyConverted = BsonUuidUtil.convertBsonBinaryToUuid(legacyBinary);
        assertEquals(maxUuid, legacyConverted);
        assertEquals("ffffffff-ffff-ffff-ffff-ffffffffffff", legacyConverted.toString());
    }

    @Test
    @DisplayName("Test hashString produces consistent results")
    void testHashStringConsistency() {
        String testString = "test-string-for-hashing";
        
        long hash1 = BsonUuidUtil.hashString(testString);
        long hash2 = BsonUuidUtil.hashString(testString);
        
        assertEquals(hash1, hash2, "Hash should be consistent for the same input");
    }

    @Test
    @DisplayName("Test hashString produces different results for different inputs")
    void testHashStringDifferentInputs() {
        String string1 = "test-string-1";
        String string2 = "test-string-2";
        
        long hash1 = BsonUuidUtil.hashString(string1);
        long hash2 = BsonUuidUtil.hashString(string2);
        
        assertNotEquals(hash1, hash2, "Different strings should produce different hashes");
    }

    @Test
    @DisplayName("Test hashString with empty string")
    void testHashStringEmpty() {
        String emptyString = "";
        
        long hash = BsonUuidUtil.hashString(emptyString);
        assertNotEquals(0L, hash, "Empty string should still produce a non-zero hash");
    }

    @Test
    @DisplayName("Test hashString with Unicode characters")
    void testHashStringUnicode() {
        String unicodeString = "Hello 世界 🌍";
        
        long hash1 = BsonUuidUtil.hashString(unicodeString);
        long hash2 = BsonUuidUtil.hashString(unicodeString);
        
        assertEquals(hash1, hash2, "Unicode strings should hash consistently");
    }

    @Test
    @DisplayName("Test conversion preserves UUID version and variant")
    void testUuidVersionAndVariantPreservation() {
        UUID v4Uuid = UUID.randomUUID();
        
        BsonBinary standardBinary = BsonUuidUtil.uuidToBsonBinary(v4Uuid);
        UUID standardConverted = BsonUuidUtil.convertBsonBinaryToUuid(standardBinary);
        assertEquals(v4Uuid.version(), standardConverted.version());
        assertEquals(v4Uuid.variant(), standardConverted.variant());
        
        BsonBinary legacyBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(v4Uuid);
        UUID legacyConverted = BsonUuidUtil.convertBsonBinaryToUuid(legacyBinary);
        assertEquals(v4Uuid.version(), legacyConverted.version());
        assertEquals(v4Uuid.variant(), legacyConverted.variant());
    }

    @Test
    @DisplayName("Test conversion preserves most and least significant bits")
    void testUuidBitsPreservation() {
        UUID originalUuid = UUID.randomUUID();
        long mostSigBits = originalUuid.getMostSignificantBits();
        long leastSigBits = originalUuid.getLeastSignificantBits();
        
        BsonBinary standardBinary = BsonUuidUtil.uuidToBsonBinary(originalUuid);
        UUID standardConverted = BsonUuidUtil.convertBsonBinaryToUuid(standardBinary);
        assertEquals(mostSigBits, standardConverted.getMostSignificantBits());
        assertEquals(leastSigBits, standardConverted.getLeastSignificantBits());
        
        BsonBinary legacyBinary = BsonUuidUtil.uuidToBsonBinaryLegacy(originalUuid);
        UUID legacyConverted = BsonUuidUtil.convertBsonBinaryToUuid(legacyBinary);
        assertEquals(mostSigBits, legacyConverted.getMostSignificantBits());
        assertEquals(leastSigBits, legacyConverted.getLeastSignificantBits());
    }

    @Test
    @DisplayName("Test BsonBinary with non-UUID subtype throws exception")
    void testNonUuidSubtypeThrowsException() {
        UUID originalUuid = UUID.randomUUID();
        byte[] uuidBytes = ByteBuffer.allocate(16)
            .putLong(originalUuid.getMostSignificantBits())
            .putLong(originalUuid.getLeastSignificantBits())
            .array();
        
        BsonBinary bsonBinary = new BsonBinary(BsonBinarySubType.USER_DEFINED, uuidBytes);
        
        // The current implementation throws an exception for non-UUID subtypes
        // when calling asUuid() on the BsonBinary
        assertThrows(Exception.class, 
            () -> BsonUuidUtil.convertBsonBinaryToUuid(bsonBinary),
            "Should throw exception for non-UUID subtype");
    }
}