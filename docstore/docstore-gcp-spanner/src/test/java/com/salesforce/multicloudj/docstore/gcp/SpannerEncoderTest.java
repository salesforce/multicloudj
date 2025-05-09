package com.salesforce.multicloudj.docstore.gcp;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Value;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SpannerEncoderTest {
    @Test
    void testEncode() {
        class TestCase {
            final Object in;
            final Value want;

            TestCase(Object in, Value want) {
                this.in = in;
                this.want = want;
            }
        }

        List<TestCase> testCases = Arrays.asList(
            new TestCase(5, Value.int64(5)),
            new TestCase(5L, Value.int64(5)),
            new TestCase("input", Value.string("input")),
            new TestCase(3.14, Value.float64(3.14)),
            new TestCase(true, Value.bool(true)),
            new TestCase(null, Value.string(null))
        );

        for (TestCase test : testCases) {
            SpannerEncoder encoder = new SpannerEncoder();
            Codec.encode(test.in, encoder);
            Value got = encoder.getValue();
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    void testEncodeList() {
        // Test integer list
        SpannerEncoder encoder = new SpannerEncoder();
        List<Integer> intList = Arrays.asList(1, 2, 3);
        Codec.encode(intList, encoder);
        Value got = encoder.getValue();
        Value expected = Value.int64Array(Arrays.asList(1L, 2L, 3L));
        Assertions.assertEquals(expected, got);

        // Test string list
        encoder = new SpannerEncoder();
        List<String> stringList = Arrays.asList("a", "b", "c");
        Codec.encode(stringList, encoder);
        got = encoder.getValue();
        expected = Value.stringArray(Arrays.asList("a", "b", "c"));
        Assertions.assertEquals(expected, got);

        // Test boolean list
        encoder = new SpannerEncoder();
        List<Boolean> boolList = Arrays.asList(true, false, true);
        Codec.encode(boolList, encoder);
        got = encoder.getValue();
        expected = Value.boolArray(Arrays.asList(true, false, true));
        Assertions.assertEquals(expected, got);

        // Test double list
        encoder = new SpannerEncoder();
        List<Double> doubleList = Arrays.asList(1.1, 2.2, 3.3);
        Codec.encode(doubleList, encoder);
        got = encoder.getValue();
        expected = Value.float64Array(Arrays.asList(1.1, 2.2, 3.3));
        Assertions.assertEquals(expected, got);
    }

    @Test
    void testEncodeMap() {
        SpannerEncoder encoder = new SpannerEncoder();
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        map.put("c", "d");
        Codec.encode(map, encoder);

        Map<String, Value> got = encoder.getMap();
        Assertions.assertEquals(2, got.size());
        Assertions.assertEquals(Value.string("b"), got.get("a"));
        Assertions.assertEquals(Value.string("d"), got.get("c"));
    }

    @Test
    void testEncodeBytes() {
        SpannerEncoder encoder = new SpannerEncoder();
        byte[] bytes = new byte[]{1, 2, 3};
        Codec.encode(bytes, encoder);
        
        Value got = encoder.getValue();
        Assertions.assertEquals(Value.bytes(ByteArray.copyFrom(bytes)), got);
    }

    @Test
    void testEncodeNull() {
        SpannerEncoder encoder = new SpannerEncoder();
        Codec.encode(null, encoder);
        
        Value got = encoder.getValue();
        Assertions.assertEquals(Value.string(null), got);
    }
} 