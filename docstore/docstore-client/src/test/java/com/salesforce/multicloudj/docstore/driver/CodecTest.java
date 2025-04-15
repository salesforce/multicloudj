package com.salesforce.multicloudj.docstore.driver;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.docstore.driver.codec.Codec;
import com.salesforce.multicloudj.docstore.driver.codec.Decoder;
import com.salesforce.multicloudj.docstore.driver.codec.Encoder;
import com.salesforce.multicloudj.docstore.driver.codec.ListDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.codec.MapDecoderCallback;
import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import lombok.Getter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CodecTest {
    @Test
    public void testEncode() throws Exception {

        // Inner static class to hold test data
        class TestCase {
            final Object in;
            final Object want;

            TestCase(Object in, Object want) {
                this.in = in;
                this.want = want;
            }
        }

        String[][] tempArray = {{"a", "b"}, {"c", "d"}};
        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
                new TestCase(5, 5),
                new TestCase(5L, 5L),
                new TestCase("input", "input"),
                new TestCase(3.14, 3.14),
                new TestCase(tempArray, new ArrayList<>(
                        Arrays.asList(
                                new ArrayList<>(Arrays.asList("a", "b")), // First inner list
                                new ArrayList<>(Arrays.asList("c", "d"))  // Second inner list
                        )
                )),
                new TestCase(Map.of("a", "b", "c", "d"), Map.of("a", "b", "c", "d")),
                new TestCase(Map.of(1, "b", 2, "d"), Map.of("1", "b", "2", "d")),
                new TestCase(Map.of(
                        "b", 2,
                        "a", Map.of("d", 2, "b", 3)
                ), Map.of(
                        "b", 2,
                        "a", Map.of("d", 2, "b", 3)
                )),
                new TestCase(new byte[]{1, 2}, new byte[]{1, 2}),
                new TestCase(new byte[]{1, 2}, new byte[]{1, 2}),
                new TestCase(false, false),
                new TestCase(Collections.singleton("a"), List.of("a")),
                new TestCase(null, null),
                new TestCase(ts, ts.toByteArray())
        );

        for (TestCase test : testCases) {
            TestEncoder encoder = new TestEncoder();
            Codec.encode(test.in, encoder);

            Object got = encoder.getVal();

            if ((got != test.want) && (!got.equals(test.want)) && !(Arrays.equals((byte[]) got, (byte[]) test.want))) {
                throw new AssertionError(String.format("Expected %s but got %s", test.want, got));
            }
        }
    }

    @Test
    public void testEncodeDecodeClass() {
        Person person = new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(100).build());
        FieldCache cache = new FieldCache();
        Book book = new Book("YellowBook", person, "WA", Timestamp.newBuilder().setNanos(1000).build(), 3.99f, new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)), null);
        TestEncoder encoder = new TestEncoder();
        Codec.encodeObject(book, cache, encoder);
        Assertions.assertNotNull(encoder.getVal());
        Assertions.assertEquals(HashMap.class, encoder.getVal().getClass());
        HashMap<?, ?> val = (HashMap<?, ?>) encoder.getVal();
        Assertions.assertEquals(6, val.size());
        Assertions.assertEquals("WA", val.get("publisher"));
        Assertions.assertEquals("YellowBook", val.get("title"));
        Assertions.assertEquals(3.990000009536743, val.get("price"));
        Assertions.assertArrayEquals(Timestamp.newBuilder().setNanos(1000).build().toByteArray(), (byte[]) val.get("publishedDate"));
        HashMap<?, ?> personMap = (HashMap<?, ?>) val.get("author");
        Assertions.assertEquals("Zoe", personMap.get("firstName"));
        Assertions.assertEquals("Ford", personMap.get("lastName"));
        Assertions.assertArrayEquals(Timestamp.newBuilder().setNanos(100).build().toByteArray(), (byte[]) personMap.get("dateOfBirth"));
        HashMap<?, ?> tableOfContents = (HashMap<?, ?>) val.get("tableOfContents");
        Assertions.assertEquals(5, tableOfContents.get("Chapter 1"));
        Assertions.assertEquals(10, tableOfContents.get("Chapter 2"));

        TestDecoder decoder = new TestDecoder(val);
        Book decodedBook = new Book();
        Assertions.assertEquals("Book(title=null, author=null, publisher=null, publishedDate=null, price=0.0, tableOfContents=null, docRevision=null)",
                decodedBook.toString(),
                "Object should be empty before decoder call");
        Codec.decodeToClass(decodedBook, cache, decoder);
        Assertions.assertEquals("Book(title=YellowBook, author=Person(nickNames=[Jamie], firstName=Zoe, lastName=Ford, dateOfBirth=nanos: 100\n" +
                "), publisher=WA, publishedDate=nanos: 1000\n" +
                ", price=3.99, tableOfContents={Chapter 1=5, Chapter 2=10}, docRevision=null)", decodedBook.toString());
        Assertions.assertEquals(book.toString(), decodedBook.toString());
    }

    @Test
    public void testDecode() {

        // Inner static class to hold test data
        class TestCase {
            final Object in;
            final Object val;
            final Object want;

            TestCase(Object in, Object val, Object want) {
                this.in = in;
                this.val = val;
                this.want = want;
            }
        }

        Timestamp ts = Timestamp.newBuilder().setNanos(1000).build();

        List<TestCase> testCases = Arrays.asList(
                new TestCase(Timestamp.getDefaultInstance(), ts.toByteArray(), ts),
                // map having list as value
                new TestCase(new HashMap<String, List<String>>(), Map.of("a", List.of("b", "c")), Map.of("a", List.of("b", "c"))),
                new TestCase(new HashMap<String, Integer>(), Map.of("b", 1), Map.of("b", 1)),
                new TestCase("some-text", "some-text", "some-text"),
                new TestCase(5, 5, 5),
                new TestCase(null, null, null),
                new TestCase(5L, 5L, 5L),
                new TestCase(5.12, 5.12, 5.12),
                new TestCase(5.5f, 5.5f, 5.5f),

                new TestCase(true, true, true),
                new TestCase(new HashMap<String, String>(), Map.of("b", "c"), Map.of("b", "c")),

                // nested map
                new TestCase(new HashMap<String, HashMap<String, String>>(), Map.of("a", Map.of("b", "c")), Map.of("a", Map.of("b", "c"))),
                new TestCase(new ArrayList<>(), List.of("a", "b"), List.of("a", "b")),
                new TestCase((new String[]{}), new String[]{"Volvo", "BMW", "Ford", "Mazda"}, new String[]{"Volvo", "BMW", "Ford", "Mazda"})
                );

        for (TestCase test : testCases) {
            TestDecoder decoder = new TestDecoder(test.val);
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
    public void testClassFieldDoesNotExist() {
            Map<String, Object> badMap = Map.of("something-does-not-exist", "bad");
            FieldCache cache = new FieldCache();
            TestDecoder decoder = new TestDecoder(badMap);
            Exception exception = Assertions.assertThrows(InvalidArgumentException.class,
                    () -> Codec.decodeToClass(new Book(), cache, decoder));
            Assertions.assertEquals("Field something-does-not-exist not found", exception.getMessage());
    }

    @Test
    public void testClassFieldDoesNotExistNestedClass() {
        Map<String, Object> goodMap = Map.of("author", Map.of("firstName", "Taylor"));
        FieldCache cache = new FieldCache();
        TestDecoder decoder = new TestDecoder(goodMap);
        Codec.decodeToClass(new Book(), cache, decoder);

        Map<String, Object> badMap = Map.of("author", Map.of("firstName", "Taylor", "badName", "Something"));
        decoder = new TestDecoder(badMap);
        TestDecoder finalDecoder = decoder;
        Exception exception = Assertions.assertThrows(InvalidArgumentException.class,
                () -> Codec.decodeToClass(new Book(), cache, finalDecoder));
        Assertions.assertEquals("Field badName not found", exception.getMessage());
    }

    @Test
    public void testDecoderObjectNullChecks() {
        Map<String, Object> goodMap = Map.of("author", Map.of("firstName", "Taylor"));
        FieldCache cache = new FieldCache();
        TestDecoder decoder = new TestDecoder(goodMap);
        Assertions.assertThrows(NullPointerException.class,
                () -> Codec.decodeToClass(null, cache, decoder));
        Assertions.assertThrows(NullPointerException.class,
                () -> Codec.decodeToClass(goodMap, null, decoder));
        Assertions.assertThrows(NullPointerException.class,
                () -> Codec.decodeToClass(goodMap, cache, null));
    }

    @Test
    public void testEncodeErrors() {

        List<TestCase> testCases = Arrays.asList(
                new TestCase("bad type", new Object()),
                new TestCase("bad type in list", List.of(new Object())),
                new TestCase("bad type in map", Collections.singletonMap(new Object(), new Object()))
        );

        for (TestCase test : testCases) {
            TestEncoder encoder = new TestEncoder();
//            Assertions.assertThrows(IllegalArgumentException.class, () -> Codec.encode(test.val, encoder));
        }
    }

    @Getter
    static class TestEncoder implements Encoder { // Assuming EncoderInterface is defined in your code
        // Getter for encoded value
        private Object val;

        @Override
        public void encodeNil() {
            val = null;
        }

        @Override
        public void encodeBool(boolean x) {
            val = x;
        }

        @Override
        public void encodeString(String x) {
            val = x;
        }

        @Override
        public void encodeInt(int x) {
            val = x;
        }

        @Override
        public void encodeLong(long x) {
            val = x;
        }

        @Override
        public void encodeFloat(double x) {
            val = x;
        }

        @Override
        public void encodeBytes(byte[] x) {
            val = x;
        }

        @Override
        public Encoder encodeList(int n) {
            List<Object> list = new ArrayList<>(n);
            val = list;
            return new ListEncoder(list);
        }

        @Override
        public Encoder encodeArray(int n) {
            List<Object> list = new ArrayList<>(n);
            val = list;
            return new ListEncoder(list);
        }

        @Override
        public void listIndex(int i) {

        }

        @Override
        public Encoder encodeMap(int n) {
            Map<String, Object> map = new HashMap<>(n);
            val = map;
            return new MapEncoder(map);
        }

        @Override
        public void mapKey(String key) {

        }
    }

    static class ListEncoder extends TestEncoder implements Encoder {
        private final List<Object> list;

        ListEncoder(List<Object> list) {
            this.list = list;
        }

        @Override
        public void listIndex(int i) {
            list.add(i, this.getVal());
        }
    }

    static class MapEncoder extends TestEncoder implements Encoder {
        private final Map<String, Object> map;

        MapEncoder(Map<String, Object> map) {
            this.map = map;
        }

        @Override
        public void mapKey(String k) {
            map.put(k, this.getVal());
        }
    }

    static class TestDecoder implements Decoder { // Assuming EncoderInterface is defined in your code
        private final Object val;
        public TestDecoder(Object val) {
            this.val = val;
        }

        @Override
        public String asString() {
            return val.toString();
        }

        @Override
        public Integer asInt() {
            return Integer.valueOf(val.toString());
        }

        @Override
        public Long asLong() {
            return Long.valueOf(val.toString());
        }

        @Override
        public Float asFloat() {
            return Float.valueOf(val.toString());
        }

        @Override
        public Double asDouble() {
            return Double.valueOf(val.toString());
        }

        @Override
        public byte[] asBytes() {
            return (byte[]) val;
        }

        @Override
        public Boolean asBool() {
            return Boolean.valueOf(val.toString());
        }

        @Override
        public Boolean asNull() {
            return val == null;
        }

        @Override
        public void decodeList(ListDecoderCallback listCallback) {
            if (val.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(val); i++) {
                    if (!listCallback.decode(i, new TestDecoder(((Object[]) val)[i]))) {
                        break;
                    }
                }
            } else {
                List<?> list = (List<?>) val;
                for (int i = 0; i < list.size(); i++) {
                    if (!listCallback.decode(i, new TestDecoder(list.get(i)))) {
                        break;
                    }
                }
            }


        }

        @Override
        public int listLen() {
            if (val.getClass().isArray()) {
                return Array.getLength(val);
            } else {
                return ((Collection<?>)val).size();
            }
        }

        @Override
        public void decodeMap(MapDecoderCallback mapDecoderCallback) {
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) val).entrySet()) {
                if (!mapDecoderCallback.decode((String) entry.getKey(), new TestDecoder(entry.getValue()))) {
                    break;
                }
            }
        }

        @Override
        public Object asInterface() {
            return val;
        }
    }

    static class TestCase {
        String desc;
        Object val;

        TestCase(String desc, Object val) {
            this.desc = desc;
            this.val = val;
        }
    }
}