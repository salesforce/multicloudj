package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class AliDecoderTest {
  @Test
  public void testDecode() {

    // Inner static class to hold test data
    class TestCase {
      final Object in;
      final ColumnValue val;
      final Object want;

      TestCase(Object in, ColumnValue val, Object want) {
        this.in = in;
        this.val = val;
        this.want = want;
      }
    }

    Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

    List<TestCase> testCases =
        Arrays.asList(
            new TestCase(
                Timestamp.getDefaultInstance(), ColumnValue.fromBinary(ts.toByteArray()), ts),
            new TestCase("some-text", ColumnValue.fromString("some-text"), "some-text"),
            new TestCase(5, ColumnValue.fromLong(5), 5),
            new TestCase(null, ColumnValue.INTERNAL_NULL_VALUE, null),
            new TestCase(5L, ColumnValue.fromLong(5L), 5L),
            new TestCase(5.12, ColumnValue.fromDouble(5.12), 5.12),
            new TestCase(5.5f, ColumnValue.fromDouble(5.5f), 5.5f),
            new TestCase(true, ColumnValue.fromBoolean(true), true));

    for (TestCase test : testCases) {
      AliDecoder decoder = new AliDecoder(test.val);
      Class<?> clazz = null;
      if (test.in != null) {
        clazz = test.in.getClass();
      }
      Object got = Codec.decode(clazz, test.in, decoder);
      if (test.in != null && test.in.getClass().isArray()) {
        Assertions.assertArrayEquals(
            (String[]) test.want,
            Arrays.stream((Object[]) got)
                .map(Object::toString) // Convert each object to string
                .toArray(String[]::new),
            String.format("Expected %s but got %s", test.want, got));
      } else if (test.in instanceof Map || test.in instanceof List) {
        Assertions.assertEquals(
            test.want, test.in, String.format("Expected %s but got %s", test.want, got));
      } else {
        Assertions.assertEquals(
            test.want, got, String.format("Expected %s but got %s", test.want, got));
      }
    }
  }

  @Test
  void testDecodeNull() {
    AliDecoder decoder = new AliDecoder(ColumnValue.INTERNAL_NULL_VALUE);
    Assertions.assertNull(decoder.asInt());
    Assertions.assertNull(decoder.asFloat());
    Assertions.assertNull(decoder.asString());
    Assertions.assertNull(decoder.asDouble());
    Assertions.assertNull(decoder.asLong());
    Assertions.assertNull(decoder.asBytes());
    Assertions.assertNull(decoder.asBool());
    Assertions.assertEquals(true, decoder.asNull());
  }

  @Test
  void testAsInterfaceNarrowsIntegerColumnToInteger() {
    Object got = new AliDecoder(ColumnValue.fromLong(5L)).asInterface();
    Assertions.assertInstanceOf(Integer.class, got);
    Assertions.assertEquals(5, got);
  }

  @Test
  void testAsInterfaceKeepsOutOfIntRangeColumnAsLong() {
    long big = 3_000_000_000L; // > Integer.MAX_VALUE
    Object got = new AliDecoder(ColumnValue.fromLong(big)).asInterface();
    Assertions.assertInstanceOf(Long.class, got);
    Assertions.assertEquals(big, got);
  }

  @Test
  void testAsInterfaceNarrowsDoubleColumnToFloat() {
    Object got = new AliDecoder(ColumnValue.fromDouble(5.5)).asInterface();
    Assertions.assertInstanceOf(Float.class, got);
    Assertions.assertEquals(5.5f, got);
  }

  @Test
  void testAsInterfaceKeepsHighPrecisionDoubleAsDouble() {
    double precise = 0.1; // not exactly representable as a float
    Object got = new AliDecoder(ColumnValue.fromDouble(precise)).asInterface();
    Assertions.assertInstanceOf(Double.class, got);
    Assertions.assertEquals(precise, got);
  }

  @Test
  void testAsInterfaceNarrowsIntegerPrimaryKeyToInteger() {
    Object got = new AliDecoder(PrimaryKeyValue.fromLong(7L)).asInterface();
    Assertions.assertInstanceOf(Integer.class, got);
    Assertions.assertEquals(7, got);
  }

  @Test
  void testAsInterfaceKeepsOutOfIntRangePrimaryKeyAsLong() {
    long big = 5_000_000_000L; // > Integer.MAX_VALUE
    Object got = new AliDecoder(PrimaryKeyValue.fromLong(big)).asInterface();
    Assertions.assertInstanceOf(Long.class, got);
    Assertions.assertEquals(big, got);
  }

  @Test
  void testAsInterfaceNarrowsNegativeIntegerColumnToInteger() {
    Object got = new AliDecoder(ColumnValue.fromLong(-5L)).asInterface();
    Assertions.assertInstanceOf(Integer.class, got);
    Assertions.assertEquals(-5, got);
  }

  @Test
  void testAsInterfaceNarrowsIntBoundaryColumnToInteger() {
    // Exact int boundaries must narrow to Integer.
    Assertions.assertInstanceOf(
        Integer.class, new AliDecoder(ColumnValue.fromLong(Integer.MAX_VALUE)).asInterface());
    Assertions.assertInstanceOf(
        Integer.class, new AliDecoder(ColumnValue.fromLong(Integer.MIN_VALUE)).asInterface());
  }

  @Test
  void testAsInterfaceKeepsJustBeyondIntBoundaryColumnAsLong() {
    // One past each int boundary must stay Long (narrowing would wrap).
    Assertions.assertInstanceOf(
        Long.class, new AliDecoder(ColumnValue.fromLong(Integer.MAX_VALUE + 1L)).asInterface());
    Assertions.assertInstanceOf(
        Long.class, new AliDecoder(ColumnValue.fromLong(Integer.MIN_VALUE - 1L)).asInterface());
  }

  @Test
  void testAsInterfaceNarrowsIntBoundaryPrimaryKeyToInteger() {
    Assertions.assertInstanceOf(
        Integer.class, new AliDecoder(PrimaryKeyValue.fromLong(Integer.MAX_VALUE)).asInterface());
    Assertions.assertInstanceOf(
        Integer.class, new AliDecoder(PrimaryKeyValue.fromLong(Integer.MIN_VALUE)).asInterface());
  }

  @Test
  void testAsInterfaceKeepsJustBeyondIntBoundaryPrimaryKeyAsLong() {
    Assertions.assertInstanceOf(
        Long.class, new AliDecoder(PrimaryKeyValue.fromLong(Integer.MAX_VALUE + 1L)).asInterface());
    Assertions.assertInstanceOf(
        Long.class, new AliDecoder(PrimaryKeyValue.fromLong(Integer.MIN_VALUE - 1L)).asInterface());
  }
}
