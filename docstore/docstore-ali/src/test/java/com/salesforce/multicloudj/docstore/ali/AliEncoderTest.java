package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliEncoderTest {
    @Test
    public void testEncode() throws Exception {

        // Inner static class to hold test data
        class TestCase {
            final Object in;
            final ColumnValue want;

            TestCase(Object in, ColumnValue want) {
                this.in = in;
                this.want = want;
            }
        }

        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
                new TestCase(5, ColumnValue.fromLong(5L)),
                new TestCase("input", ColumnValue.fromString("input")),
                new TestCase(3.14, ColumnValue.fromDouble(3.14)),
                new TestCase(ts, ColumnValue.fromBinary(ts.toByteArray()))
        );

        for (TestCase test : testCases) {
            AliEncoder encoder = new AliEncoder();
            Codec.encode(test.in, encoder);

            ColumnValue got = encoder.getColumnValue();

            if ((got != test.want) && (!got.equals(test.want)) ) {
                throw new AssertionError(String.format("Expected %s but got %s", test.want, got));
            }
        }
    }

    @Test
    void testEncodeList() {
        AliEncoder encoder = new AliEncoder();
        List<Integer> list = Arrays.asList(1, 2, 3);
        Codec.encode(list, encoder);
        Assertions.assertEquals(3, encoder.getList().size());
        Assertions.assertEquals(ColumnValue.fromLong(1), encoder.getList().get(0));
        Assertions.assertEquals(ColumnValue.fromLong(2), encoder.getList().get(1));
        Assertions.assertEquals(ColumnValue.fromLong(3), encoder.getList().get(2));
    }

    @Test
    void testEncodeArray() {
        AliEncoder encoder = new AliEncoder();
        Integer[] array = {4, 5, 6};
        Codec.encode(array, encoder);
        Assertions.assertEquals(3, encoder.getList().size());
        Assertions.assertEquals(ColumnValue.fromLong(4), encoder.getList().get(0));
        Assertions.assertEquals(ColumnValue.fromLong(5), encoder.getList().get(1));
        Assertions.assertEquals(ColumnValue.fromLong(6), encoder.getList().get(2));
    }

    @Test
    void testEncodeMap() {
        AliEncoder encoder = new AliEncoder();
        Map<String, String> map = new HashMap<>();
        map.put("a", "b");
        Codec.encode(map, encoder);
        Assertions.assertEquals(1, encoder.getMap().size());
        Assertions.assertEquals(ColumnValue.fromString("b"), encoder.getMap().get("a"));
    }
}
