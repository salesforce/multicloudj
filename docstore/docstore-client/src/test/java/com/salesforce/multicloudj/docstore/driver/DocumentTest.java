package com.salesforce.multicloudj.docstore.driver;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DocumentTest {
    private static Document docMap = null;
    private static Document docObject = null;

    @BeforeAll
    public static void setup() {
        Map<String, Object> bookMap = new HashMap<>();
        Person person = new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(1000).build());
        bookMap.put("title", "YellowBook");
        bookMap.put("author", person);
        bookMap.put("publisher", "WA");
        bookMap.put("publishedDate", new Date(2020, Calendar.DECEMBER, 1));
        bookMap.put("price", 3.99f);
        bookMap.put("tableOfContents", new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)));
        docMap = new Document(bookMap);

        Book book = new Book("YellowBook", person, "WA", Timestamp.newBuilder().setNanos(1000).build(), 3.99f, new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)), null);
        docObject = new Document(book);
    }

    @Test
    public void testNullDocument() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> {new Document(null);});
    }

    @Test
    public void testGetField() {
        Person p = (Person) docMap.getField("author");
        assertEquals("Zoe", p.getFirstName());
        p = (Person) docObject.getField("author");
        assertEquals("Zoe", p.getFirstName());
    }

    @Test
    public void testSetField() {
        docMap.setField("title", "BlueBook");
        assertEquals("BlueBook", docMap.getField("title"));
        docObject.setField("title", "BlueBook");
        assertEquals("BlueBook", docObject.getField("title"));
        assertThrows(RuntimeException.class, () -> docObject.setField("badField", "bark"));
    }

    @Test
    public void testGetDocument() {
        Document doc = docMap.getDocument(new String[]{"author", "firstName"}, false);
        assertTrue(doc.getFieldNames().size() >= 9);

        /* TODO: This test should be successful to support collections as fields
        doc = docMap.getDocument(new String[]{"author", "nickNames"}, false);
        assertNotNull(doc);
        */
        // Test cannot create a new field in an object.
        assertThrows(IllegalArgumentException.class, () -> docMap.getDocument(new String[]{"author", "badInput"}, true));

        // Test can add a new field in a map and return an empty document.
        doc = docMap.getDocument(new String[]{"tableOfContents", "Chapter 3"}, true);
        assertNotNull(doc);

        doc = docObject.getDocument(new String[]{"author", "firstName"}, false);
        assertTrue(doc.getFieldNames().size() >= 9);


        // Test cannot create a new field in an object.
        assertThrows(IllegalArgumentException.class, () -> docObject.getDocument(new String[]{"author", "badInput"}, true));

        // Test can add a new field in a map and return an empty document.
        doc = docObject.getDocument(new String[]{"tableOfContents", "Chapter 3"}, true);
        assertNotNull(doc);
    }

    @Test
    public void testGet() {
        assertEquals ("YellowBook", docMap.get(new String[]{"title"}));
        assertEquals ("Zoe", docMap.get(new String[]{"author", "firstName"}));
        assertEquals (5, docMap.get(new String[]{"tableOfContents", "Chapter 1"}));

        assertEquals ("YellowBook", docObject.get(new String[]{"title"}));
        assertEquals ("Zoe", docObject.get(new String[]{"author", "firstName"}));
        assertEquals (5, docObject.get(new String[]{"tableOfContents", "Chapter 1"}));
    }

    @Test
    public void testSet() {
        docMap.set(new String[]{"title"}, "BlueBook");
        assertEquals ("BlueBook", docMap.get(new String[]{"title"}));
        docMap.set(new String[]{"author", "firstName"}, "Zack");
        assertEquals ("Zack", docMap.get(new String[]{"author", "firstName"}));
        assertThrows(IllegalArgumentException.class, () -> docMap.set(new String[]{"author", "initialis"}, "ZF"));
        docMap.set(new String[]{"tableOfContents", "Chapter 1"}, 6);
        assertEquals (6, docMap.get(new String[]{"tableOfContents", "Chapter 1"}));
        docMap.set(new String[]{"tableOfContents", "Chapter 3"}, 14);
        assertEquals (14, docMap.get(new String[]{"tableOfContents", "Chapter 3"}));

        docObject.set(new String[]{"title"}, "BlueBook");
        assertEquals ("BlueBook", docObject.get(new String[]{"title"}));
        docObject.set(new String[]{"author", "firstName"}, "Zack");
        assertEquals ("Zack", docObject.get(new String[]{"author", "firstName"}));
        assertThrows(IllegalArgumentException.class, () -> docObject.set(new String[]{"author", "initialis"}, "ZF"));
        docObject.set(new String[]{"tableOfContents", "Chapter 1"}, 6);
        assertEquals (6, docObject.get(new String[]{"tableOfContents", "Chapter 1"}));
        docObject.set(new String[]{"tableOfContents", "Chapter 3"}, 14);
        assertEquals (14, docObject.get(new String[]{"tableOfContents", "Chapter 3"}));
    }

    @Test
    public void testFieldNames() {
        assertEquals(6, docMap.getFieldNames().size());
        assertEquals("author", docMap.getFieldNames().get(0));
        assertEquals(7, docObject.getFieldNames().size());
        assertEquals("author", docObject.getFieldNames().get(0));
    }

    @Test
    public void testEncodeDecodeObject() {
        CodecTest.TestEncoder encoder = new CodecTest.TestEncoder();
        docObject.encode(encoder);
        CodecTest.TestDecoder decoder = new CodecTest.TestDecoder(encoder.getVal());
        FieldCache cache = new FieldCache();
        Book decBook = new Book();
        Document decDoc = new Document(decBook);
        decDoc.decode(decoder);
        Assertions.assertEquals(docObject.getField("publisher"), decDoc.getField("publisher"));
    }

}