package com.salesforce.multicloudj.docstore.aws;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AwsEncoderTest {
    @Test
    public void testEncode() throws Exception {

        // Inner static class to hold test data
        class TestCase {
            final Object in;
            final AttributeValue want;

            TestCase(Object in, AttributeValue want) {
                this.in = in;
                this.want = want;
            }
        }

        String[][] tempArray = {{"a", "b"}, {"c", "d"}};
        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
                new TestCase(5, AttributeValue.builder().n("5").build()),
                new TestCase(5L, AttributeValue.builder().n(Long.toString(5L)).build()),
                new TestCase("input", AttributeValue.builder().s("input").build()),
                new TestCase(3.14, AttributeValue.builder().n("3.14").build()),
//                new TestCase(tempArray, AttributeValue.builder().l(
//                        AttributeValue.builder().l(AttributeValue.builder().s("a").build(), AttributeValue.builder().s("b").build()).build(),
//                        AttributeValue.builder().l(AttributeValue.builder().s("c").build(), AttributeValue.builder().s("d").build()).build()
//                ).build()),
//                new TestCase(Map.of("a", "b", "c", "d"), Map.of("a", "b", "c", "d")),
//                new TestCase(Map.of(1, "b", 2, "d"), Map.of("1", "b", "2", "d")),
//                new TestCase(Map.of(
//                        "b", 2,
//                        "a", Map.of("d", 2, "b", 3)
//                ), Map.of(
//                        "b", 2,
//                        "a", Map.of("d", 2, "b", 3)
//                )),
//                new TestCase(new byte[]{1, 2}, new byte[]{1, 2}),
//                new TestCase(new byte[]{1, 2}, new byte[]{1, 2}),
//                new TestCase(false, false),
//                new TestCase(Collections.singleton("a"), List.of("a")),
//                new TestCase(null, null),
                new TestCase(ts, AttributeValue.builder().b(SdkBytes.fromByteArray(ts.toByteArray())).build())
        );

        for (TestCase test : testCases) {
            AwsEncoder encoder = new AwsEncoder();
            Codec.encode(test.in, encoder);

            AttributeValue got = encoder.getAttributeValue();

            if ((got != test.want) && (!got.equals(test.want)) ) {
                throw new AssertionError(String.format("Expected %s but got %s", test.want, got));
            }
        }
    }

    @Test
    void testEncodeList() throws Exception {
        AwsEncoder encoder = new AwsEncoder();
        List<Integer> list = Arrays.asList(1, 2, 3);
        Codec.encode(list, encoder);
        // To add assert when the function is supported.
    }

    @Test
    void testEncodeArray() throws Exception {
        AwsEncoder encoder = new AwsEncoder();
        Integer[] array = {1, 2, 3};
        Codec.encode(array, encoder);
        // To add assert when the function is supported.
    }

    @Test
    void testEncodeMap() throws Exception {
        AwsEncoder encoder = new AwsEncoder();
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        Codec.encode(map, encoder);
        // To add assert when the function is supported.
    }
}
