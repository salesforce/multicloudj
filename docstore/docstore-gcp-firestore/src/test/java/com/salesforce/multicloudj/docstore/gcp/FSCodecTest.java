package com.salesforce.multicloudj.docstore.gcp;

import com.google.firestore.v1.Document;
import com.google.firestore.v1.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FSCodecTest {
    static Value nullValue = Value.newBuilder()
        .setNullValue(com.google.protobuf.NullValue.NULL_VALUE)
        .build();

    static Value stringValue(String s) {
        return Value.newBuilder().setStringValue(s).build();
    }

    static Value intValue(long n) {
        return Value.newBuilder().setIntegerValue(n).build();
    }

    static Value doubleValue(double d) {
        return Value.newBuilder().setDoubleValue(d).build();
    }

    static Value boolValue(boolean b) {
        return Value.newBuilder().setBooleanValue(b).build();
    }

    static Value mapValue(Map<String, Value> map) {
        return Value.newBuilder()
            .setMapValue(com.google.firestore.v1.MapValue.newBuilder()
                .putAllFields(map)
                .build())
            .build();
    }

    @Test
    public void testEncodeValue() {
        class TestCase {
            Object in;
            Value want;

            TestCase(Object in, Value want) {
                this.in = in;
                this.want = want;
            }
        }

        Integer seven = 7;
        List<TestCase> tests = Arrays.asList(
            new TestCase(null, nullValue),
            new TestCase(0, intValue(0)),
            new TestCase(999L, intValue(999)),
            new TestCase(3.5, doubleValue(3.5)),
            new TestCase("", stringValue("")),
            new TestCase("x", stringValue("x")),
            new TestCase(true, boolValue(true)),
            new TestCase(seven, intValue(7))
        );

        for (TestCase test : tests) {
            Value got = FSCodec.encodeValue(test.in);
            Assertions.assertEquals(test.want, got);
        }
    }

    @Test
    public void testEncodeDoc() {
        HashMap<String, Object> m = new HashMap<>();
        com.salesforce.multicloudj.docstore.driver.Document doc = new com.salesforce.multicloudj.docstore.driver.Document(m);
        doc.setField("string", "value");
        doc.setField("number", 42);
        doc.setField("boolean", true);

        Map<String, Value> encoded = FSCodec.encodeDoc(doc);
        Assertions.assertEquals(3, encoded.size());
        Assertions.assertEquals(stringValue("value"), encoded.get("string"));
        Assertions.assertEquals(intValue(42), encoded.get("number"));
        Assertions.assertEquals(boolValue(true), encoded.get("boolean"));
    }

    @Test
    public void testDecodeDoc() {
        // Create a Firestore Document with some fields
        Document fsDoc = Document.newBuilder()
            .putFields("string", stringValue("value"))
            .putFields("number", intValue(42))
            .putFields("boolean", boolValue(true))
            .build();

        // Create a target document
        Map<String, Object> m = new HashMap<>();
        com.salesforce.multicloudj.docstore.driver.Document doc = new com.salesforce.multicloudj.docstore.driver.Document(m);
        // Decode the snapshot into the document
        FSCodec.decodeDoc(fsDoc, doc);

        // Verify the decoded values
        Assertions.assertEquals("value", doc.getField("string"));
        Assertions.assertEquals(42, doc.getField("number"));
        Assertions.assertEquals(true, doc.getField("boolean"));
    }
} 