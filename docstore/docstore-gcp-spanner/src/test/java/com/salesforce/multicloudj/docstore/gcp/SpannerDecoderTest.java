package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SpannerDecoderTest {
    @Test
    void testDecode() {
        class TestCase {
            final Object want;
            final Type type;

            TestCase(Object want, Type type) {
                this.want = want;
                this.type = type;
            }
        }

        List<TestCase> testCases = Arrays.asList(
            new TestCase(5, Type.int64()),
            new TestCase(5L, Type.int64()),
            new TestCase("input", Type.string()),
            new TestCase(3.14, Type.float64()),
            new TestCase(true, Type.bool())
        );

        for (TestCase test : testCases) {
            Value value;
            if (test.want instanceof Integer) {
                value = Value.int64((Integer) test.want);
            } else if (test.want instanceof Long) {
                value = Value.int64((Long) test.want);
            } else if (test.want instanceof String) {
                value = Value.string((String) test.want);
            } else if (test.want instanceof Double) {
                value = Value.float64((Double) test.want);
            } else if (test.want instanceof Boolean) {
                value = Value.bool((Boolean) test.want);
            } else {
                throw new IllegalArgumentException("Unsupported type: " + test.want.getClass());
            }

            Struct struct = Struct.newBuilder()
                .set("value").to(value)
                .build();

            SpannerDecoder decoder = new SpannerDecoder(struct);
            decoder.setValue(struct.getValue(0));
            Object got = Codec.decode(test.want.getClass(), null, decoder);
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    void testDecodeList() {
        // This test is currently skipped as decodeList is marked as TODO
        // TODO: Re-enable this test once decodeList is implemented
    }

    @Test
    void testDecodeMap() {
        Map<String, String> values = new HashMap<>();
        values.put("a", "b");
        values.put("c", "d");

        Struct struct = Struct.newBuilder()
                .set("a").to("b")
                .set("c").to("d")
                .build();

        SpannerDecoder decoder = new SpannerDecoder(struct);
        //decoder.setValue(struct.getValue(0));

        Map<String, String> want = Map.of("a", "b", "c", "d");
        Map<String, String> got = new HashMap<>();
        Codec.decode(want.getClass(), got, decoder);
        Assertions.assertEquals(want, got);
    }

    @Test
    void testDecodeNull() {
        Struct struct = Struct.newBuilder()
            .set("value").to(Value.string(null))
            .build();

        SpannerDecoder decoder = new SpannerDecoder(struct);
        decoder.setValue(struct.getValue(0));

        Object got = Codec.decode(Object.class, null, decoder);
        Assertions.assertNull(got);
    }

    @Test
    void testValueConstructor() {
        Value value = Value.string("test");
        SpannerDecoder decoder = new SpannerDecoder(value);
        Assertions.assertEquals("test", decoder.asString());
    }
} 