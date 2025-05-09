package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.Struct;
import com.google.protobuf.ByteString;
import com.salesforce.multicloudj.docstore.driver.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class SpannerCodecTest {
    static Value nullValue = Value.string(null);

    static Value stringValue(String s) {
        return Value.string(s);
    }

    static Value intValue(long n) {
        return Value.int64(n);
    }

    static Value doubleValue(double d) {
        return Value.float64(d);
    }

    static Value boolValue(boolean b) {
        return Value.bool(b);
    }

    @Test
    void testEncodeValue() {
        class TestCase {
            Object in;
            Value want;

            TestCase(Object in, Value want) {
                this.in = in;
                this.want = want;
            }
        }

        Integer seven = 7;
        TestCase[] tests = new TestCase[]{
            new TestCase(null, nullValue),
            new TestCase(0, intValue(0)),
            new TestCase(999L, intValue(999)),
            new TestCase(3.5, doubleValue(3.5)),
            new TestCase("", stringValue("")),
            new TestCase("x", stringValue("x")),
            new TestCase(true, boolValue(true)),
            new TestCase(seven, intValue(7))
        };

        for (TestCase test : tests) {
            Value got = (Value) SpannerCodec.encodeValue(test.in);
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    public void testToValueAllCases() {
        // Test integer conversions
        Value intValue = Value.int64(5);
        SpannerDecoder intDecoder = new SpannerDecoder(intValue);
        Assertions.assertEquals(5L, intDecoder.asInterface());

        Value longValue = Value.int64(Integer.MAX_VALUE + 1L);
        SpannerDecoder longDecoder = new SpannerDecoder(longValue);
        Assertions.assertEquals(Integer.MAX_VALUE + 1L, longDecoder.asInterface());

        // Test double conversions
        Value floatCompatibleDouble = Value.float32(3.5f);
        SpannerDecoder floatDecoder = new SpannerDecoder(floatCompatibleDouble);
        Assertions.assertEquals(3.5f, floatDecoder.asInterface());

        Value preciseDouble = Value.float64(3.14159265359);
        SpannerDecoder doubleDecoder = new SpannerDecoder(preciseDouble);
        Assertions.assertEquals(3.14159265359, doubleDecoder.asInterface());

        // Test string
        Value stringValue = Value.string("test");
        SpannerDecoder stringDecoder = new SpannerDecoder(stringValue);
        Assertions.assertEquals("test", stringDecoder.asInterface());

        // Test bytes
        byte[] testBytes = {1, 2, 3};
        Value bytesValue = Value.bytes(ByteArray.copyFrom(testBytes));
        SpannerDecoder bytesDecoder = new SpannerDecoder(bytesValue);
        Assertions.assertArrayEquals(testBytes, (byte[]) bytesDecoder.asInterface());

        // Test boolean
        Value boolValue = Value.bool(true);
        SpannerDecoder boolDecoder = new SpannerDecoder(boolValue);
        Assertions.assertEquals(true, boolDecoder.asInterface());

        // Test map
        Value mapValue = Value.struct(Struct.newBuilder().set("a").to(1).build());

        SpannerDecoder mapDecoder = new SpannerDecoder(mapValue);
        Map<String, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1L);
        Assertions.assertEquals(expectedMap, mapDecoder.asInterface());

        // Test null
        SpannerDecoder nullDecoder = new SpannerDecoder(nullValue);
        Assertions.assertNull(nullDecoder.asInterface());
    }

    @Test
    public void testValueDecoders() {
        // Test string conversion fallbacks
        Value intValue = Value.int64(42);
        SpannerDecoder decoder = new SpannerDecoder(intValue);
        Assertions.assertEquals(42, decoder.asInt());

        Value stringVal = Value.string("42");
        decoder = new SpannerDecoder(stringVal);
        Assertions.assertEquals("42", decoder.asString());

        Value longVal = Value.int64(42);
        decoder = new SpannerDecoder(longVal);
        Assertions.assertEquals(42L, decoder.asLong());

        Value floatVal = Value.float32(3.14f);
        decoder = new SpannerDecoder(floatVal);
        Assertions.assertEquals(3.14f, decoder.asFloat());

        Value doubleVal = Value.float64(3.14d);
        decoder = new SpannerDecoder(doubleVal);
        Assertions.assertEquals(3.14d, decoder.asDouble());

        Value boolVal = Value.bool(true);
        decoder = new SpannerDecoder(boolVal);
        Assertions.assertTrue(decoder.asBool());

        Value byteValue = Value.bytes(ByteArray.copyFrom("byteValue".getBytes()));
        decoder = new SpannerDecoder(byteValue);
        Assertions.assertArrayEquals("byteValue".getBytes(), decoder.asBytes());
    }
    
    @Test
    public void testEncodeDocKeyFields() {
        // Create a document with key fields
        Map<String, Object> m = new HashMap<>();
        Document doc = new Document(m);
        doc.setField("id", "123");
        doc.setField("sort", "abc");

        // Test with both primary and sort key
        Map<String, Value> got = SpannerCodec.encodeDocKeyFields(doc, "id", "sort");
        Assertions.assertEquals(2, got.size());
        Assertions.assertEquals(stringValue("123"), got.get("id"));
        Assertions.assertEquals(stringValue("abc"), got.get("sort"));

        // Test with only primary key
        got = SpannerCodec.encodeDocKeyFields(doc, "id", null);
        Assertions.assertEquals(1, got.size());
        Assertions.assertEquals(stringValue("123"), got.get("id"));

        // Test with missing primary key
        got = SpannerCodec.encodeDocKeyFields(doc, "missing", null);
        Assertions.assertNull(got);
    }

    @Test
    public void testEncodeDoc() {
        // Create a document with various field types
        Map<String, Object> m = new HashMap<>();
        Document doc = new Document(m);
        doc.setField("string", "value");
        doc.setField("number", 42);
        doc.setField("boolean", true);

        Map<String, Value> encoded = SpannerCodec.encodeDoc(doc);
        Assertions.assertEquals(3, encoded.size());
        Assertions.assertEquals(stringValue("value"), encoded.get("string"));
        Assertions.assertEquals(intValue(42), encoded.get("number"));
        Assertions.assertEquals(boolValue(true), encoded.get("boolean"));
    }

    @Test
    public void testDecodeDoc() {
        // Create a Spanner Struct with some fields
        Map<String, Value> fields = new HashMap<>();
        fields.put("string", stringValue("value"));
        fields.put("number", intValue(42));
        fields.put("boolean", boolValue(true));
        Struct struct = Struct.newBuilder().set("string").to("value")
                                         .set("number").to(42L)
                                         .set("boolean").to(true)
                                         .build();

        // Create a target document
        Map<String, Object> m = new HashMap<>();
        Document doc = new Document(m);
        
        // Decode the struct into the document
        SpannerCodec.decodeDoc(struct, doc);

        // Verify the decoded values
        Assertions.assertEquals("value", doc.getField("string"));
        Assertions.assertEquals(42L, doc.getField("number"));
        Assertions.assertEquals(true, doc.getField("boolean"));
    }
}