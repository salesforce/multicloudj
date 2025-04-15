package com.salesforce.multicloudj.docstore.aws;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsDecoderTest {
    @Test
    public void testDecode() {

        // Inner static class to hold test data
        class TestCase {
            final Object in;
            final AttributeValue val;
            final Object want;

            TestCase(Object in, AttributeValue val, Object want) {
                this.in = in;
                this.val = val;
                this.want = want;
            }
        }

        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
                new TestCase(Timestamp.getDefaultInstance(), AttributeValue.builder().b(SdkBytes.fromByteArray(ts.toByteArray())).build(), ts),
                // map having list as value
                new TestCase(new HashMap<String, List<String>>(), AttributeValue.builder().m(Map.of("a", AttributeValue.builder().l(List.of(AttributeValue.builder().s("b").build(), AttributeValue.builder().s("c").build())).build())).build(), Map.of("a", List.of("b", "c"))),
                new TestCase(new HashMap<String, Integer>(), AttributeValue.builder().m(Map.of("b", AttributeValue.builder().n("1").build())).build(), Map.of("b", 1)),
                new TestCase("some-text", AttributeValue.builder().s("some-text").build(), "some-text"),
                new TestCase(5, AttributeValue.builder().n("5").build(), 5),
                new TestCase(null, AttributeValue.builder().nul(true).build(), null),
                new TestCase(5L, AttributeValue.builder().n(Long.toString(5L)).build(), 5L),
                new TestCase(5.12, AttributeValue.builder().n("5.12").build(), 5.12),
                new TestCase(5.5f, AttributeValue.builder().n("5.5f").build(), 5.5f),

                new TestCase(true, AttributeValue.builder().bool(true).build(), true),
                new TestCase(new HashMap<String, String>(), AttributeValue.builder().m(Map.of("b", AttributeValue.builder().s("c").build())).build(), Map.of("b", "c"))

                // nested map
                //new TestCase(new HashMap<String, HashMap<String, String>>(), Map.of("a", Map.of("b", "c")), Map.of("a", Map.of("b", "c")))
                //new TestCase(new ArrayList<>(), AttributeValue.builder().l(AttributeValue.builder().s("a").build(), AttributeValue.builder().s("b").build()).build(), List.of("a", "b"))
                //new TestCase((new String[]{}), AttributeValue.builder().l(new ArrayList<>(List.of(AttributeValue.builder().s("Volvo").build(),
//                        AttributeValue.builder().s("BMW").build(),
//                        AttributeValue.builder().s("Ford").build(),
//                        AttributeValue.builder().s("Mazda").build()))).build(), new String[]{"Volvo", "BMW", "Ford", "Mazda"})
        );

        for (TestCase test : testCases) {
            AwsDecoder decoder = new AwsDecoder(test.val);
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
        AwsDecoder decoder = new AwsDecoder(AttributeValue.builder().nul(true).build());
        Assertions.assertNull(decoder.asString());
        Assertions.assertNull(decoder.asInt());
        Assertions.assertNull(decoder.asLong());
        Assertions.assertNull(decoder.asFloat());
        Assertions.assertNull(decoder.asDouble());
        Assertions.assertNull(decoder.asBytes());
        Assertions.assertNull(decoder.asBool());
        Assertions.assertEquals(true, decoder.asNull());
    }
}
