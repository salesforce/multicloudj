package com.salesforce.multicloudj.docstore.aws;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.salesforce.multicloudj.docstore.driver.codec.Codec.decode;
import static com.salesforce.multicloudj.docstore.driver.codec.Codec.encode;


public class AwsCodecTest {
    static AttributeValue nullValue = AttributeValue.builder().nul(true).build();

    static AttributeValue avn(String n) {
        return AttributeValue.builder().n(n).build();
    }

    static AttributeValue av() {
        return AttributeValue.builder().build();
    }

    static AttributeValue avl(AttributeValue... values) {
        return AttributeValue.builder().l(Arrays.asList(values)).build();
    }

    @Test
    public void testEncodeValue() {
        class TestCase {
            Object in;
            AttributeValue want;

            TestCase(Object in, AttributeValue want) {
                this.in = in;
                this.want = want;
            }
        }

        Integer seven = 7;
        List<TestCase> tests = Arrays.asList(
                new TestCase(null, nullValue),
                new TestCase(0, avn("0")),
                new TestCase(999L, avn("999")),
                new TestCase(3.5, avn("3.5")),
                new TestCase("", nullValue),
                new TestCase("x", AttributeValue.builder().s("x").build()),
                new TestCase(true, AttributeValue.builder().bool(true).build()),
                new TestCase(seven, avn("7")),
//                new TestCase(new int[]{}, av().toBuilder().l(Collections.emptyList()).build()),
//                new TestCase(Arrays.asList(1, 2), avl(avn("1"), avn("2"))),
//                new TestCase(Arrays.asList(null, false), avl(nullValue, AttributeValue.builder().bool(false).build())),
                new TestCase(new HashMap<String, Integer>(), av().toBuilder().m(new HashMap<>()).build()),
                new TestCase(Map.of("a", 1, "b", 2), AttributeValue.builder()
                        .m(Map.of(
                                "a", avn("1"),
                                "b", avn("2")))
                        .build())
        );

        for (TestCase test : tests) {
            AwsEncoder encoder = new AwsEncoder();
            encode(test.in, encoder);
            AttributeValue got = encoder.getAttributeValue();
            Assertions.assertEquals(got, test.want);
        }
    }

    @Test
    public void testDecodeUnsupported() {
        class TestCase {
            AttributeValue in;
            Object out;

            TestCase(AttributeValue in, Object out) {
                this.in = in;
                this.out = out;
            }
        }

        List<TestCase> tests = Arrays.asList(
                new TestCase(AttributeValue.builder().ss("foo", "bar").build(), new ArrayList<String>()),
                new TestCase(AttributeValue.builder().ns("1.1", "-2.2", "3.3").build(), new ArrayList<Double>()),
                new TestCase(AttributeValue.builder().bs(SdkBytes.fromByteBuffer(ByteBuffer.wrap(new byte[]{4})), SdkBytes.fromByteBuffer(ByteBuffer.wrap(new byte[]{5})), SdkBytes.fromByteBuffer(ByteBuffer.wrap(new byte[]{6}))).build(), new ArrayList<byte[]>())
        );

        for (TestCase tc : tests) {
            AwsDecoder d = new AwsDecoder(tc.in);
            decode(tc.out.getClass(), tc.out, d);  // Curently we don't have decodelist. It should fail as unsupported here when we have it.
            ArrayList<?> out = (ArrayList<?>)tc.out;
            Assertions.assertEquals(0, out.size());
        }
    }


}
