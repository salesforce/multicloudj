package com.salesforce.multicloudj.blob.inmemory;

/**
 * CRC-64/ECMA-182 implementation, used only by the in-memory test-double provider to validate
 * caller-supplied CRC64 checksums.
 *
 * <p>Package-private on purpose: this is a hand-rolled checksum for the in-memory provider's
 * local validation only and must not be reachable from production provider code, which relies on
 * its own cloud SDK for checksums.
 *
 * <p>Variant: polynomial {@code 0x42F0E1EBA9EA3693} (normal/MSB-first form), init {@code 0},
 * non-reflected, no final XOR. Published check value for ASCII {@code "123456789"} is
 * {@code 0x6C40DF5F0B497347} (asserted by the unit test). This is the simplest CRC-64 variant
 * (no reflection / no init/xor masks), which minimizes the chance of error in a hand-rolled
 * implementation.
 *
 * <p>The in-memory provider computes and validates a checksum entirely locally — it never sends
 * the value to, or compares it against, a real storage backend. Only internal self-consistency
 * matters here, so this value does not need to match the exact CRC-64 variant any particular cloud
 * substrate computes on the wire (some use a reflected, masked variant). The substrate drivers
 * keep using their own SDKs' checksum implementations.
 */
final class Crc64 {

  private static final long POLY = 0x42F0E1EBA9EA3693L;
  private static final long[] TABLE = new long[256];

  static {
    for (int b = 0; b < 256; b++) {
      long crc = (long) b << 56;
      for (int i = 0; i < 8; i++) {
        crc = (crc & 0x8000000000000000L) != 0 ? (crc << 1) ^ POLY : (crc << 1);
      }
      TABLE[b] = crc;
    }
  }

  private Crc64() {}

  /** Computes the CRC-64/ECMA-182 checksum of the given bytes. */
  static long compute(byte[] data) {
    long crc = 0L;
    for (byte datum : data) {
      int index = (int) (((crc >>> 56) ^ datum) & 0xFF);
      crc = TABLE[index] ^ (crc << 8);
    }
    return crc;
  }
}
