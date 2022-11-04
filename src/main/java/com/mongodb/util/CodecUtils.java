/* (C) Copyright 2013, MongoDB, Inc. */

package com.mongodb.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.zip.CRC32;

import net.jpountz.xxhash.StreamingXXHash32;
import net.jpountz.xxhash.XXHashFactory;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;

/** The codec utils. */
public final class CodecUtils {

  private static final String MD5_DIGEST = "MD5";
  private static final String SHA256_DIGEST = "SHA-256";

  private static final String ENCODING = "UTF-8";
  private static final int xxHashSeed = 1234;

  public static long xxh3Hash(final byte[] bytes) {
    return LongHashFunction.xx3().hashBytes(bytes);
  }

  public static String sha256Hex(final String pV) {
    try {
      final MessageDigest d = MessageDigest.getInstance(SHA256_DIGEST);
      d.reset();
      d.update(pV.getBytes(ENCODING));
      return Hex.encodeHexString(d.digest());
    } catch (Throwable t) {
      throw new IllegalStateException(t);
    }
  }

  public static String md5Hex(final String pV) {
    try {
      return md5Hex(pV.getBytes(ENCODING));
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static String md5Hex(final byte[] pV) {
    return md5Hex(pV, 0, pV.length);
  }

  public static String md5Hex(final byte[] pV, final int pOffset, final int pLength) {
    final MessageDigest d = sCache.get();
    d.update(pV, pOffset, pLength);
    return Hex.encodeHexString(d.digest());
  }

  public static byte[] md5(final byte[] pV) {
    final MessageDigest d = sCache.get();
    d.update(pV);
    return d.digest();
  }

  public static byte[] sha1(final byte[] pV) throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA-1").digest(pV);
  }

  public static long crc32(final String pV) {
    final CRC32 hasher = _crc32Hasher.get();

    try {
      hasher.update(pV.getBytes(ENCODING));
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }

    return hasher.getValue();
  }

  public static String toHex(final byte[] pV) {
    try {
      return Hex.encodeHexString(pV);
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  /** Converts a 16 byte md5 value to a hex string. */
  public static String md5ToHex(final byte[] pV) {
    try {
      if (pV.length == MD5_BYTE_SIZE) return Hex.encodeHexString(pV);

      final byte[] buffer = _16ByteBuffer.get();

      System.arraycopy(pV, 0, buffer, 0, MD5_BYTE_SIZE);

      return Hex.encodeHexString(buffer);

    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static String toHex(final String pV) {
    try {
      return Hex.encodeHexString(pV.getBytes(ENCODING));
    } catch (final UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static byte[] fromHex(final String pV) {
    try {
      return Hex.decodeHex(pV.toCharArray());
    } catch (final Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  public static String toBase64(final byte[] pV) {
    return Base64.encodeBase64String(pV);
  }

  public static String toBase64UrlSafe(final byte[] pV) {
    return Base64.encodeBase64URLSafeString(pV);
  }

  public static byte[] fromBase64(final String pV) {
    return Base64.decodeBase64(pV);
  }

  public static String toBase32(final byte[] pV) {
    return new Base32().encodeAsString(pV);
  }

  public static byte[] fromBase32(final String pV) {
    return new Base32().decode(pV);
  }

  public static final int MD5_BYTE_SIZE = 16;

  private static ThreadLocal<byte[]> _16ByteBuffer =
      new ThreadLocal<byte[]>() {
        @Override
        public final byte[] initialValue() {
          return new byte[MD5_BYTE_SIZE];
        }
      };

  private static ThreadLocal<MessageDigest> sCache =
      new ThreadLocal<MessageDigest>() {
        @Override
        public final MessageDigest initialValue() {
          try {
            return MessageDigest.getInstance(MD5_DIGEST);
          } catch (final Throwable t) {
            throw new IllegalStateException(t);
          }
        }

        @Override
        public final MessageDigest get() {
          final MessageDigest d = super.get();
          d.reset();
          return d;
        }
      };

  private static ThreadLocal<CRC32> _crc32Hasher =
      new ThreadLocal<CRC32>() {
        @Override
        public final CRC32 initialValue() {
          try {
            return new CRC32();
          } catch (final Throwable t) {
            throw new IllegalStateException(t);
          }
        }

        @Override
        public final CRC32 get() {
          final CRC32 d = super.get();
          d.reset();
          return d;
        }
      };
}
