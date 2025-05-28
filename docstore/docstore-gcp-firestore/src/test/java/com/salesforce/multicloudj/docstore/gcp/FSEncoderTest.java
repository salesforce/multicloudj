package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Value;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FSEncoderTest {
    @Test
    public void testEncode() {
        class TestCase {
            final Object in;
            final Value want;

            TestCase(Object in, Value want) {
                this.in = in;
                this.want = want;
            }
        }

        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
            new TestCase(5, Value.newBuilder().setIntegerValue(5).build()),
            new TestCase(5L, Value.newBuilder().setIntegerValue(5).build()),
            new TestCase("input", Value.newBuilder().setStringValue("input").build()),
            new TestCase(3.14, Value.newBuilder().setDoubleValue(3.14).build()),
            new TestCase(true, Value.newBuilder().setBooleanValue(true).build()),
            new TestCase(ts, Value.newBuilder().setBytesValue(ts.toByteString()).build())
        );

        for (TestCase test : testCases) {
            FSEncoder encoder = new FSEncoder();
            Codec.encode(test.in, encoder);
            Value got = encoder.getValue();
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    public void testEncodeList() {
        FSEncoder encoder = new FSEncoder();
        List<Integer> list = Arrays.asList(1, 2, 3);
        Codec.encode(list, encoder);
        
        Value got = encoder.getValue();
        Assertions.assertTrue(got.hasArrayValue());
        Assertions.assertEquals(3, got.getArrayValue().getValuesCount());
        Assertions.assertEquals(1, got.getArrayValue().getValues(0).getIntegerValue());
        Assertions.assertEquals(2, got.getArrayValue().getValues(1).getIntegerValue());
        Assertions.assertEquals(3, got.getArrayValue().getValues(2).getIntegerValue());
    }

    @Test
    public void testEncodeMap() {
        FSEncoder encoder = new FSEncoder();
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        map.put("c", "d");
        Codec.encode(map, encoder);

        Value got = encoder.getValue();
        Assertions.assertTrue(got.hasMapValue());
        Assertions.assertEquals(2, got.getMapValue().getFieldsCount());
        Assertions.assertEquals("b", got.getMapValue().getFieldsMap().get("a").getStringValue());
        Assertions.assertEquals("d", got.getMapValue().getFieldsMap().get("c").getStringValue());
    }

    @Test
    public void testEncodeNull() {
        FSEncoder encoder = new FSEncoder();
        Codec.encode(null, encoder);
        
        Value got = encoder.getValue();
        Assertions.assertTrue(got.hasNullValue());
        Assertions.assertEquals(com.google.protobuf.NullValue.NULL_VALUE, got.getNullValue());
    }
} 