package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

        List<TestCase> testCases = Arrays.asList(
                new TestCase(Timestamp.getDefaultInstance(), ColumnValue.fromBinary(ts.toByteArray()), ts),
                new TestCase("some-text", ColumnValue.fromString("some-text"), "some-text"),
                new TestCase(5, ColumnValue.fromLong(5), 5),
                new TestCase(null, ColumnValue.INTERNAL_NULL_VALUE, null),
                new TestCase(5L, ColumnValue.fromLong(5L), 5L),
                new TestCase(5.12, ColumnValue.fromDouble(5.12), 5.12),
                new TestCase(5.5f, ColumnValue.fromDouble(5.5f), 5.5f),

                new TestCase(true, ColumnValue.fromBoolean(true), true)
        );

        for (TestCase test : testCases) {
            AliDecoder decoder = new AliDecoder(test.val);
            Class<?> clazz = null;
            if (test.in != null) {
                clazz = test.in.getClass();
            }
            Object got = Codec.decode(clazz, test.in, decoder);
            if (test.in != null && test.in.getClass().isArray()) {
                Assertions.assertArrayEquals((String[])test.want, Arrays.stream((Object[])got)
                        .map(Object::toString)  // Convert each object to string
                        .toArray(String[]::new), String.format("Expected %s but got %s", test.want, got));
            } else if (test.in instanceof Map || test.in instanceof List) {
                Assertions.assertEquals(test.want, test.in, String.format("Expected %s but got %s", test.want, got));
            } else {
                Assertions.assertEquals(test.want, got, String.format("Expected %s but got %s", test.want, got));
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
}
