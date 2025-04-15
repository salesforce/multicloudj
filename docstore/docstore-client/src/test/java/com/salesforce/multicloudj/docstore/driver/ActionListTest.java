package com.salesforce.multicloudj.docstore.driver;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class ActionListTest {

    AbstractDocStore mockDocStore = mock(AbstractDocStore.class);
    ActionList al = null;
    Document mockDoc = mock(Document.class);

    @BeforeEach
    void setUp() {
        al = new ActionList(mockDocStore);
        Assertions.assertNotNull(al);
    }

    @Test
    void testCreate() {
        al.create(mockDoc);
        Assertions.assertEquals(1, al.getActions().size());
    }

    @Test
    void testCreateWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.create(mockDoc).enableAtomicWrites().create(mockDoc1).create(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testReplace() {
        al.replace(mockDoc);
        Assertions.assertEquals(1, al.getActions().size());
    }

    @Test
    void testReplaceWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.replace(mockDoc).enableAtomicWrites().replace(mockDoc1).replace(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testPut() {
        al.put(mockDoc);
        Assertions.assertEquals(1, al.getActions().size());
    }

    @Test
    void testPutWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.put(mockDoc).enableAtomicWrites().put(mockDoc1).put(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testDelete() {
        al.delete(mockDoc);
        Assertions.assertEquals(1, al.getActions().size());
    }

    @Test
    void testDeleteWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.delete(mockDoc).enableAtomicWrites().delete(mockDoc1).delete(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testMixWritesWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.delete(mockDoc).enableAtomicWrites().put(mockDoc1).create(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }


    @Test
    void testGet() {
        al.get(mockDoc, "Path");
        Assertions.assertEquals(1, al.getActions().size());
        Assertions.assertEquals("Path", al.getActions().get(0).getFieldPaths().get(0));
    }

    @Test
    void testGetWithTxNoImpact() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.delete(mockDoc).enableAtomicWrites().get(mockDoc1).get(mockDoc2);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertFalse(al.getActions().get(1).inAtomicWrite);
        Assertions.assertFalse(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testUpdate() {
        al.update(mockDoc, null);
        Assertions.assertEquals(1, al.getActions().size());
        Assertions.assertNull(al.getActions().get(0).getMods());
    }

    @Test
    void testUpdateWithTx() {
        Document mockDoc1 = mock(Document.class);
        Document mockDoc2 = mock(Document.class);
        al.update(mockDoc, null).enableAtomicWrites().update(mockDoc1, null).update(mockDoc2, null);
        Assertions.assertEquals(3, al.getActions().size());
        Assertions.assertFalse(al.getActions().get(0).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(1).inAtomicWrite);
        Assertions.assertTrue(al.getActions().get(2).inAtomicWrite);
    }

    @Test
    void testBeforeDo() {
        Consumer mockConsumer = mock(Consumer.class);
        al.beforeDo(mockConsumer);
        Assertions.assertEquals(0, al.getActions().size());
    }

    @Test
    void testToDriverActions() {
        doNothing().when(mockDocStore).runActions(any(), any());
        doReturn(InvalidArgumentException.class).when(mockDocStore).getException(any(IllegalArgumentException.class));
        al.create(mockDoc).run();
        Assertions.assertEquals(0, al.getActions().get(0).getIndex());

        Document doc = new Document(new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(100).build()));
        doNothing().when(mockDocStore).toDriverAction(any());

        al.create(doc).create(doc).run();
        al.getActions().get(0).setKey("FakeKey");
        al.getActions().get(1).setKey("FakeKey");
        al.getActions().get(0).setKind(ActionKind.ACTION_KIND_CREATE);
        al.getActions().get(1).setKind(ActionKind.ACTION_KIND_CREATE);
        Assertions.assertThrows(InvalidArgumentException.class, () -> al.run());
    }
}
