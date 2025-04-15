package com.salesforce.multicloudj.docstore.driver;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class UtilTest {
    class TestAction extends Action {
        public TestAction(ActionKind kind,
                          Document document,
                          Object key,
                          List<String> fieldPaths,
                          Map<String, Object> mods,
                          boolean atomicWrites) {
            super(kind, document, fieldPaths, mods, atomicWrites);
            this.setKey(key);
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

    @Test
    public void testGroupActions() {
        Person person = new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(100).build());

        List<Action> actions = new ArrayList<>();

        List<Action> beforeGets = new ArrayList<>();
        List<Action> getList = new ArrayList<>();
        List<Action> writeList = new ArrayList<>();
        List<Action> atomicWriteList = new ArrayList<>();
        List<Action> afterGets = new ArrayList<>();

        TestAction nullKey = new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(person), null, null, null, false);

        TestAction bget = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), "Ford", null, null, false);

        TestAction write = new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(person), "Ford", null, null, false);

        TestAction atomicWrite1 = new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(person), "Subaru",null, null, true);

        TestAction atomicWrite2 = new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(person), "Benz",null, null, true);

        TestAction cget = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), "TOYOTA",null, null, false);

        TestAction aget = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), "Ford",null, null, false);

        actions.add(nullKey);
        actions.add(bget);
        actions.add(write);
        actions.add(atomicWrite1);
        actions.add(atomicWrite2);
        actions.add(cget);
        actions.add(aget);

        Util.groupActions(actions, beforeGets, getList, writeList, atomicWriteList, afterGets);
        Assertions.assertEquals(1, beforeGets.size());
        Assertions.assertEquals(1, getList.size());
        Assertions.assertEquals(2, writeList.size());
        Assertions.assertEquals(2, atomicWriteList.size());
        Assertions.assertEquals(1, afterGets.size());
    }

    @Test
    public void testGroupByFieldPath() {
        List<String> fp1 = new ArrayList<>(List.of("a.b", "a.c"));
        List<String> fp2 = new ArrayList<>(List.of("a.c", "a.b"));  // Different order of fp1, treated as a different fp.
        List<String> fp3 = new ArrayList<>(List.of("d"));
        List<String> fp4 = new ArrayList<>(List.of("a.b", "c.d"));
        List<String> fp5 = new ArrayList<>(List.of("a"));

        Person person = new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(100).build());
        List<Action> gets = new ArrayList<>();

        TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp1, null, false);
        TestAction get2 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp2, null, false);
        TestAction get3 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp3, null, false);
        TestAction get4 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp3, null, false);
        TestAction get5 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp4, null, false);
        TestAction get6 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, fp5, null, false);

        gets.add(get1);
        gets.add(get2);
        gets.add(get3);
        gets.add(get4);
        gets.add(get5);
        gets.add(get6);

        List<List<Action>> groups = Util.groupByFieldPath(gets);
        Assertions.assertEquals(5, groups.size());
    }

    @Test
    void testSerializeObject() {
        Person person = new Person(Collections.singleton("Jamie"), "Zoe", "Ford", Timestamp.newBuilder().setNanos(100).build());
        Assertions.assertEquals("{\"nickNames\":[\"Jamie\"],\"firstName\":\"Zoe\",\"lastName\":\"Ford\",\"dateOfBirth\":\"1970-01-01T00:00:00.000000100Z\"}", Util.serializeObject(person));

        class MyClass {
            private String name;  // No getter method for `name`
        }
        MyClass myClass = new MyClass();
        Assertions.assertThrows(IllegalArgumentException.class, () -> Util.serializeObject(myClass));
    }

    @Test
    void testValidateFieldPath() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> Util.validateFieldPath(null));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Util.validateFieldPath(""));
        Assertions.assertThrows(IllegalArgumentException.class, () -> Util.validateFieldPath("a..b"));
        Assertions.assertDoesNotThrow(() -> Util.validateFieldPath("a.b.c"));
    }
}
