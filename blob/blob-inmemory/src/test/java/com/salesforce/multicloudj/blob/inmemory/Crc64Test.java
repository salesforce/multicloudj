package com.salesforce.multicloudj.blob.inmemory;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class Crc64Test {

  @Test
  void matchesPublishedEcma182CheckValue() {
    // The published CRC-64/ECMA-182 check value for the ASCII string "123456789".
    // This pins the implementation to the intended variant (polynomial 0x42F0E1EBA9EA3693,
    // init 0, non-reflected, no final XOR) independent of how it was written.
    long actual = Crc64.compute("123456789".getBytes(StandardCharsets.US_ASCII));
    assertEquals(0x6C40DF5F0B497347L, actual);
  }

  @Test
  void emptyInputIsZero() {
    assertEquals(0L, Crc64.compute(new byte[0]));
  }
}
