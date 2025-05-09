package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Value;
import com.google.protobuf.ByteString;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FSDecoderTest {
    @Test
    public void testDecode() {
        class TestCase {
            final Value in;
            final Object want;

            TestCase(Value in, Object want) {
                this.in = in;
                this.want = want;
            }
        }

        List<TestCase> testCases = Arrays.asList(
                new TestCase(Value.newBuilder().setIntegerValue(5).build(), 5),
                new TestCase(Value.newBuilder().setIntegerValue(5L).build(), 5L),
                new TestCase(Value.newBuilder().setStringValue("input").build(), "input"),
                new TestCase(Value.newBuilder().setDoubleValue(3.14).build(), 3.14),
                new TestCase(Value.newBuilder().setBooleanValue(true).build(), true)
        );

        for (TestCase test : testCases) {
            FSDecoder decoder = new FSDecoder(test.in);
            Object got = Codec.decode(test.want.getClass(), null, decoder);
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    public void testDecodeList() {
        Value listValue = Value.newBuilder()
                .setArrayValue(com.google.firestore.v1.ArrayValue.newBuilder()
                        .addValues(Value.newBuilder().setIntegerValue(1).build())
                        .addValues(Value.newBuilder().setIntegerValue(2).build())
                        .addValues(Value.newBuilder().setIntegerValue(3).build())
                        .build())
                .build();

        List<Integer> want = Arrays.asList(1, 2, 3);
        FSDecoder decoder = new FSDecoder(listValue);
        List<Integer> got = (List<Integer>) Codec.decode(want.getClass(), new ArrayList<>(), decoder);
        Assertions.assertEquals(want, got);
    }

    @Test
    public void testDecodeMap() {
        Value mapValue = Value.newBuilder()
                .setMapValue(com.google.firestore.v1.MapValue.newBuilder()
                        .putFields("a", Value.newBuilder().setStringValue("b").build())
                        .putFields("c", Value.newBuilder().setStringValue("d").build())
                        .build())
                .build();

        Map<String, String> want = Map.of("a", "b", "c", "d");
        FSDecoder decoder = new FSDecoder(mapValue);
        Map<String, String> got = (Map<String, String>) Codec.decode(want.getClass(), new HashMap<>(), decoder);
        Assertions.assertEquals(want, got);
    }

    @Test
    public void testDecodeNull() {
        Value nullValue = Value.newBuilder()
                .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
                .build();

        FSDecoder decoder = new FSDecoder(nullValue);
        Object got = Codec.decode(Object.class, null, decoder);
        Assertions.assertNull(got);
    }

    @Test
    public void testNullDecoder() {
        // Test with null Value
        FSDecoder decoder = new FSDecoder(null);

        // Test all methods with null value
        Assertions.assertNull(decoder.asString());
        Assertions.assertNull(decoder.asInt());
        Assertions.assertNull(decoder.asLong());
        Assertions.assertNull(decoder.asFloat());
        Assertions.assertNull(decoder.asDouble());
        Assertions.assertNull(decoder.asBytes());
        Assertions.assertNull(decoder.asBool());
        Assertions.assertTrue(decoder.asNull());

        // Test decoder interface methods
        Assertions.assertEquals(0, decoder.listLen());

        AtomicBoolean callbackCalled = new AtomicBoolean(false);
        decoder.decodeList((i, d) -> {
            callbackCalled.set(true);
            return true;
        });
        Assertions.assertFalse(callbackCalled.get());

        callbackCalled.set(false);
        decoder.decodeMap((k, d) -> {
            callbackCalled.set(true);
            return true;
        });
        Assertions.assertFalse(callbackCalled.get());

        // Test asInterface with null
        Assertions.assertNull(decoder.asInterface());
    }

    @Test
    public void testToValueAllCases() {
        // Test integer conversions
        Value intValue = Value.newBuilder().setIntegerValue(5).build();
        FSDecoder intDecoder = new FSDecoder(intValue);
        Assertions.assertEquals(5, intDecoder.asInterface());

        Value longValue = Value.newBuilder().setIntegerValue(Integer.MAX_VALUE + 1L).build();
        FSDecoder longDecoder = new FSDecoder(longValue);
        Assertions.assertEquals(Integer.MAX_VALUE + 1L, longDecoder.asInterface());

        // Test double conversions
        Value floatCompatibleDouble = Value.newBuilder().setDoubleValue(3.5).build();
        FSDecoder floatDecoder = new FSDecoder(floatCompatibleDouble);
        Assertions.assertEquals(3.5f, floatDecoder.asInterface());

        Value preciseDouble = Value.newBuilder().setDoubleValue(3.14159265359).build();
        FSDecoder doubleDecoder = new FSDecoder(preciseDouble);
        Assertions.assertEquals(3.14159265359, doubleDecoder.asInterface());

        // Test string
        Value stringValue = Value.newBuilder().setStringValue("test").build();
        FSDecoder stringDecoder = new FSDecoder(stringValue);
        Assertions.assertEquals("test", stringDecoder.asInterface());

        // Test bytes
        byte[] testBytes = {1, 2, 3};
        Value bytesValue = Value.newBuilder().setBytesValue(ByteString.copyFrom(testBytes)).build();
        FSDecoder bytesDecoder = new FSDecoder(bytesValue);
        Assertions.assertArrayEquals(testBytes, (byte[]) bytesDecoder.asInterface());

        // Test boolean
        Value boolValue = Value.newBuilder().setBooleanValue(true).build();
        FSDecoder boolDecoder = new FSDecoder(boolValue);
        Assertions.assertEquals(true, boolDecoder.asInterface());

        // Test array (list)
        Value arrayValue = Value.newBuilder()
                .setArrayValue(com.google.firestore.v1.ArrayValue.newBuilder()
                        .addValues(Value.newBuilder().setStringValue("a").build())
                        .addValues(Value.newBuilder().setIntegerValue(1).build())
                        .build())
                .build();
        FSDecoder arrayDecoder = new FSDecoder(arrayValue);
        List<Object> expectedList = Arrays.asList("a", 1);
        Assertions.assertEquals(expectedList, arrayDecoder.asInterface());

        // Test map
        Value mapValue = Value.newBuilder()
                .setMapValue(com.google.firestore.v1.MapValue.newBuilder()
                        .putFields("key1", Value.newBuilder().setStringValue("value1").build())
                        .putFields("key2", Value.newBuilder().setIntegerValue(2).build())
                        .build())
                .build();
        FSDecoder mapDecoder = new FSDecoder(mapValue);
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("key1", "value1");
        expectedMap.put("key2", 2);
        Assertions.assertEquals(expectedMap, mapDecoder.asInterface());

        // Test null
        Value nullValue = Value.newBuilder().setNullValue(com.google.protobuf.NullValue.NULL_VALUE).build();
        FSDecoder nullDecoder = new FSDecoder(nullValue);
        Assertions.assertNull(nullDecoder.asInterface());
    }

    @Test
    public void testValueDecoders() {
        // Test string conversion fallbacks
        Value intVal = Value.newBuilder().setIntegerValue(42).build();
        FSDecoder decoder = new FSDecoder(intVal);
        Assertions.assertEquals(42, decoder.asInt());

        Value stringVal = Value.newBuilder().setStringValue("42").build();
        decoder = new FSDecoder(stringVal);
        Assertions.assertEquals("42", decoder.asString());

        Value longVal = Value.newBuilder().setIntegerValue(42).build();
        decoder = new FSDecoder(longVal);
        Assertions.assertEquals(42L, decoder.asLong());

        Value floatVal = Value.newBuilder().setDoubleValue(3.14).build();
        decoder = new FSDecoder(floatVal);
        Assertions.assertEquals(3.14f, decoder.asFloat());
        Assertions.assertEquals(3.14d, decoder.asDouble());

        Value boolVal = Value.newBuilder().setBooleanValue(true).build();
        decoder = new FSDecoder(boolVal);
        Assertions.assertTrue(decoder.asBool());

        Value byteValue = Value.newBuilder().setBytesValue(ByteString.copyFrom("byteValue".getBytes())).build();
        decoder = new FSDecoder(byteValue);
        Assertions.assertArrayEquals("byteValue".getBytes(), decoder.asBytes());
    }

    @Test
    public void testListDecodeEarlyBreak() {
        Value listValue = Value.newBuilder()
                .setArrayValue(com.google.firestore.v1.ArrayValue.newBuilder()
                        .addValues(Value.newBuilder().setIntegerValue(1).build())
                        .addValues(Value.newBuilder().setIntegerValue(2).build())
                        .addValues(Value.newBuilder().setIntegerValue(3).build())
                        .build())
                .build();

        FSDecoder decoder = new FSDecoder(listValue);

        AtomicInteger callCount = new AtomicInteger(0);
        decoder.decodeList((i, d) -> {
            callCount.incrementAndGet();
            return i < 1; // Break after second element
        });

        Assertions.assertEquals(2, callCount.get());
    }

    @Test
    public void testMapDecodeEarlyBreak() {
        Value mapValue = Value.newBuilder()
                .setMapValue(com.google.firestore.v1.MapValue.newBuilder()
                        .putFields("a", Value.newBuilder().setStringValue("1").build())
                        .putFields("b", Value.newBuilder().setStringValue("2").build())
                        .putFields("c", Value.newBuilder().setStringValue("3").build())
                        .build())
                .build();

        FSDecoder decoder = new FSDecoder(mapValue);

        AtomicInteger callCount = new AtomicInteger(0);
        decoder.decodeMap((k, d) -> {
            callCount.incrementAndGet();
            return callCount.get() <= 2; // Break after second element
        });

        Assertions.assertEquals(3, callCount.get());
    }

    @Test
    public void testInvalidCases() {
        // Test invalid bytes
        Value stringValue = Value.newBuilder().setStringValue("not bytes").build();
        FSDecoder decoder = new FSDecoder(stringValue);
        Assertions.assertThrows(IllegalArgumentException.class, decoder::asBytes);

        // Test unsupported value type by creating a reference value, which isn't supported in toValue
        Value unusualValue = Value.getDefaultInstance();
        decoder = new FSDecoder(unusualValue);
        Assertions.assertThrows(IllegalArgumentException.class, decoder::asInterface);
    }
} 