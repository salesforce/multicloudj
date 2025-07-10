package com.salesforce.multicloudj.docstore.client;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.util.common.TestsUtil;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class AbstractDocstoreIT {
    // Define the Harness interface
    public interface Harness extends AutoCloseable {
        // Method to create a docstore store driver
        AbstractDocStore createDocstoreDriver(CollectionKind collectionKind);

        // Method to get the revision Id for a provider, firestore leverages
        // timestamp as revision
        Object getRevisionId();

        // provide the STS endpoint in provider
        String getDocstoreEndpoint();

        // Wiremock server need the https port, if
        // we make it constant at abstract class, we won't be able
        // to run tests in parallel. Each provider can provide the
        // randomly selected port number.
        int getPort();

        // If you need to provide extensions to wiremock proxy
        // provide the fully qualified class names here.
        List<String> getWiremockExtensions();

        // Method to check if the provider supports OrderBy in full table scans
        // Returns true if the provider can handle OrderBy clauses during full table scans,
        // false if it requires an index or fallback mechanism
        default boolean supportOrderByInFullScan() {
            return false;
        }
    }

    protected abstract Harness createHarness();


    private Harness harness;

    /**
     * Initializes the WireMock server before all tests.
     */
    @BeforeAll
    public void initializeWireMockServer() {
        Random random = new Random(12345L);
        Util.setUuidSupplier(() -> new UUID(random.nextLong(), random.nextLong()).toString());
        harness = createHarness();
        TestsUtil.startWireMockServer("src/test/resources", harness.getPort());
    }

    /**
     * Shuts down the WireMock server after all tests.
     */
    @AfterAll
    public void shutdownWireMockServer() throws Exception {
        TestsUtil.stopWireMockServer();
        harness.close();
    }

    /**
     * Initialize the harness and
     */
    @BeforeEach
    public void setupTestEnvironment() {
        TestsUtil.startWireMockRecording(harness.getDocstoreEndpoint());
        clearCollection(CollectionKind.SINGLE_KEY);
        //clearCollection(CollectionKind.TWO_KEYS);
    }

    private void clearCollection(CollectionKind collectionKind) {
        try (AbstractDocStore docStore = harness.createDocstoreDriver(collectionKind)) {
            DocStoreClient docStoreClient = new DocStoreClient(docStore);
            DocumentIterator iter = docStoreClient.query().get();
            ActionList actionList = docStoreClient.getActions();

            while (iter.hasNext()) {
                Map<?, ?> m = new HashMap<>();
                Document mDoc = new Document(m);
                iter.next(mDoc);
                actionList.delete(mDoc);
            }
            actionList.run();
        }
    }

    /**
     * Cleans up the test environment after each test.
     */
    @AfterEach
    public void cleanupTestEnvironment() {
        TestsUtil.stopWireMockRecording();
    }

    private Object newDocument(Object doc) {
        if (doc instanceof Map) {
            Map<String, Object> newDoc = new HashMap<>();
            newDoc.put("pName", ((Map<?, ?>) doc).get("pName"));
            return newDoc;
        } else {
            Player player = new Player();
            player.setPName(((Player) doc).getPName());
            return player;
        }
    }

    @Test
    public void testCreate() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        class TestCase {
            final String name;
            final Object doc;
            final Class<? extends Exception> wantErr;

            TestCase(String name, Object doc, Class<? extends Exception> wantErr) {
                this.name = name;
                this.doc = doc;
                // Treat null as OK.
                this.wantErr = wantErr;
            }
        }

        // Build doc from the user defined class
        List<TestCase> testCases = new ArrayList<>();
        Player player = new Player("JohnMap", 121, 12.66f, true, "randomString");
        testCases.add(new TestCase("CreateWithClass", player, null));

        // Key already exists for class
        Player player2 = new Player("JohnMap", 111, 12.66f, true, "randomStringMap");
        testCases.add(new TestCase("CreateAlreadyExistsWithClass", player2, ResourceAlreadyExistsException.class));

        // Key empty partition key not allowed
        Player player3 = new Player("", 111, 12.66f, true, "randomStringMap");
        testCases.add(new TestCase("CreateWithClassEmptyPartitionKey", player3, InvalidArgumentException.class));

        // Build doc from the map
        Map m = Map.of("pName", "TaylorMap", "i", 121, "f", 12.66f, "b", true, "s", "randomString");
        m = new HashMap(m);
        m.put("DocstoreRevision", null);
        testCases.add(new TestCase("CreateWithMap", m, null));

        // Key already exists for map
        Map m2 = Map.of("pName", "TaylorMap", "i", 111, "f", 12.66f, "b", true, "s", "randomString2");
        m2 = new HashMap(m2);
        m2.put("DocstoreRevision", null);
        testCases.add(new TestCase("CreateWithMapAlreadyExists", m2, ResourceAlreadyExistsException.class));

        Map m3 = Map.of("pName", "LeoMap", "i", 121, "f", 12.66f, "b", true, "DocstoreRevision", "someRevision");
        m3 = new HashMap(m3);
        testCases.add(new TestCase("CreateWithNonEmptyRevision", m3, InvalidArgumentException.class));

        Map m4 = Map.of("pName", "LeoMap", "i", 121, "f", 12.66f, "b", true);
        m4 = new HashMap(m4);
        m4.put("DocstoreRevision", null);
        testCases.add(new TestCase("CreateWithNullRevision", m4, null));

        for (TestCase testCase : testCases) {
            if (testCase.wantErr != null) {
                Assertions.assertThrows(
                        testCase.wantErr,
                        () -> docStoreClient.create(new Document(testCase.doc)),
                        String.format("Test %s failed", testCase.name));
            } else {
                verifyNoRevisionField(testCase.doc, "DocstoreRevision");
                docStoreClient.create(new Document(testCase.doc));
                verifyRevisionFieldExist(testCase.doc, "DocstoreRevision");
                Object got = newDocument(testCase.doc);
                docStoreClient.get(new Document(got));
                if (got instanceof Map) {
                    Assertions.assertTrue(compareMaps((Map<String, Object>) got, (Map<String, Object>) testCase.doc));
                } else {
                    Assertions.assertEquals(testCase.doc, got, String.format("Test %s failed", testCase.name));
                }
            }
        }
        docStoreClient.close();
    }

    public boolean compareMaps(Map<String, Object> map1, Map<String, Object> map2) {
        if (map1.size() != map2.size()) {
            return false;
        }

        for (Map.Entry<String, Object> entry : map1.entrySet()) {
            Object value1 = entry.getValue();
            Object value2 = map2.get(entry.getKey());

            if (value1 instanceof Double || value1 instanceof Float) {
                if (value2 instanceof Double || value2 instanceof Float) {
                    double d1 = value1 instanceof Double ? (Double) value1 : (double) ((Float) value1);
                    double d2 = value2 instanceof Double ? (Double) value2 : (double) ((Float) value2);

                    if (Math.abs(d1 - d2) > 0.000001) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else if (value1 instanceof Integer || value1 instanceof Long) {
                if (value2 instanceof Integer || value2 instanceof Long) {
                    if (!Objects.equals(toLong(value1), toLong(value2))) {
                        return false;
                    }
                } else {
                    return false;
                }

            } else if (value1 instanceof Timestamp && value2 instanceof Timestamp) {
                return Timestamps.compare((Timestamp) value1, (Timestamp) value2) == 0;
            }
            else if (!value1.equals(value2)) {
                return false;
            }
        }

        return true;
    }

    private static long toLong(Object obj) {
        return obj instanceof Number ? ((Number) obj).longValue() : 0L;
    }

    @Test
    public void testGet() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        class TestCase {
            final String name;
            final Object doc;
            final List<String> fieldPaths;
            final Object got;
            final Object want;
            final Class<? extends Exception> wantErr;

            TestCase(String name, Object doc, List<String> fieldPaths, Object got, Object want, Class<? extends Exception> wantErr) {
                this.name = name;
                this.doc = doc;
                this.fieldPaths = fieldPaths;
                this.got = got;
                this.want = want;
                // Treat null as OK.
                this.wantErr = wantErr;
            }
        }

        List<TestCase> testCases = new ArrayList<>();
        // 1. Test case for getting a map with no field paths (full map is returned)
        Map m = Map.of("pName", "PumaMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        Object mGot = newDocument(m);
        Map mWant = m; // expect get full map if no fields are passed
        testCases.add(new TestCase("GetWithMap", m, null, mGot, mWant, null));

        // 2. Test case for getting a class object with no field paths (full object is returned)
        Player player = new Player("BishopPlayer", 121, 12.66f, true, "random String");
        Object playerGot = newDocument(player);
        Player playerWant = player; // expect get full class if no fields are passed
        testCases.add(new TestCase("GetWithClass", player, null, playerGot, playerWant, null));

        // 3. Test case for getting a class with specific field paths (pass some fields)
        Player player1 = new Player("PrincePlayer", 121, 12.66f, true, "random String");
        List<String> playerFields1 = List.of("i", "f");
        Object playerGot1 = newDocument(player1);
        Player playerWant1 = new Player("PrincePlayer", 121, 12.66f, false, null);
        testCases.add(new TestCase("GetSomeFieldsWithClass", player1, playerFields1, playerGot1, playerWant1, null));

        // 4. Test case for getting a class with specific field paths (pass all fields)
        Player player2 = new Player("EdwardPlayer", 95, 32.3f, true, "random String");
        List<String> playerFields2 = List.of("i", "f", "b", "s");
        Object playerGot2 = newDocument(player2);
        Player playerWant2 = new Player("EdwardPlayer", 95, 32.3f, true, "random String");
        testCases.add(new TestCase("GetAllFieldsWithClass", player2, playerFields2, playerGot2, playerWant2, null));

        // 5. Test case for getting a map with specific field paths (pass some fields)
        Map m1 = Map.of("pName", "FrankMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        List<String> mFields1 = List.of("i", "f", "s");
        Object mGot1 = newDocument(m1);
        Map mWant1 = Map.of("pName", "FrankMap", "i", 121, "f", 12.66f, "s", "random String");
        testCases.add(new TestCase("GetSomeFieldsWithMap", m1, mFields1, mGot1, mWant1, null));

        // 6. Test case for getting a map with specific field paths (pass aLL fields)
        Map m2 = Map.of("pName", "DavidMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        List<String> mFields2 = List.of("i", "f", "b", "s");
        Object mGot2 = newDocument(m2);
        Map mWant2 = Map.of("pName", "DavidMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        testCases.add(new TestCase("GetAllFieldsWithMap", m2, mFields2, mGot2, mWant2, null));

        // TODO: re-enable once CAG supports embedded map
        // 7. Test case for getting a map of sub-map with specific field paths (pass some fields, plus sub-map fields)
        //Map m3 = Map.of("pName", "KingMap", "i", 121, "f", 12.66f,"s", "randomString", "m", Map.of("a", "one", "b", "two"));
        //List<String> mFields3 = List.of("i", "f", "s", "m.a");
        //Object mGot3 = newDocument(m3);
        //Map mWant3 = Map.of("pName", "KingMap", "i", 121, "f", 12.66f, "s", "randomString", "m", Map.of("a", "one"));
        //testCases.add(new TestCase("GetSomeFieldsWithMapAndSubMap", m3, mFields3, mGot3, mWant3, null));

        // 8. Test case for incorrect field path (wrong case - fields are case-sensitive)
        Map m4 = Map.of("pName", "QueenMap", "i", 121, "f", 12.66f, "s", "randomString");
        List<String> mFields4 = List.of("I", "f", "S");
        Object mGot4 = newDocument(m4);
        Map mWant4 = Map.of("pName", "QueenMap", "f", 12.66f);
        testCases.add(new TestCase("GetWrongFieldsWithMap", m4, mFields4, mGot4, mWant4, null));

        // 9. Test case for invalid argument (call get with empty partition key)
        Map m5 = Map.of("pName", "TigerMap", "i", 121, "f", 12.66f, "s", "randomString");
        List<String> mFields5 = List.of("i", "f");
        Map mGot5 = Map.of("pName", "");
        testCases.add(new TestCase("GetNoKeyWithMapReturnException", m5, mFields5, mGot5, null, InvalidArgumentException.class));

        // 10. Test case for nonexistent document (not found - No Data)
        Map m6 = Map.of("pName", "LionMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        List<String> mFields6 = List.of("i", "f");
        Map mGot6 = Map.of("pName", "NonExistent");
        Map mWant6 = Map.of("pName", "NonExistent");
        testCases.add(new TestCase("GetNonExistentWithMapReturnNoData", m6, mFields6, mGot6, mWant6, null));

        // 11. Test case for getting a map with specific field paths (created with null revision)
        Map m7 = Map.of("pName", "SharkMap", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        m7 = new HashMap(m7);
        m7.put("DocstoreRevision", null);
        List<String> mFields7 = List.of("i", "f", "s");
        Object mGot7 = newDocument(m7);
        Map mWant7 = Map.of("pName", "SharkMap", "i", 121, "f", 12.66f, "s", "random String");
        testCases.add(new TestCase("GetSomeFieldsWithMapWithNullRevision", m7, mFields7, mGot7, mWant7, null));

        for (TestCase testCase : testCases) {
            docStoreClient.create(new Document(testCase.doc));
            Object got = testCase.got;

            if (testCase.wantErr != null) {
                Assertions.assertThrows(
                        testCase.wantErr,
                        () -> docStoreClient.get(new Document(got), testCase.fieldPaths.toArray(new String[0])),
                        String.format("Test %s failed", testCase.name));
            } else {
                if (ObjectUtils.isEmpty(testCase.fieldPaths)) {
                    docStoreClient.get(new Document(got));
                } else {
                    docStoreClient.get(new Document(got), testCase.fieldPaths.toArray(new String[0]));
                }
                if (got instanceof Map) {
                    Assertions.assertTrue(compareMaps((Map) got, (Map) testCase.want));
                } else {
                    Assertions.assertEquals(testCase.want, got, String.format("Test %s failed", testCase.name));
                }
            }
        }
        docStoreClient.close();
    }

    private void verifyNoRevisionField(Object doc, String revisionField) {
        if (doc instanceof Map) {
            String revision = (String) ((Map) doc).get(revisionField);
            Assertions.assertTrue(StringUtils.isEmpty(revision));
        }
    }

    private void verifyRevisionFieldExist(Object doc, String revisionField) {
        if (doc instanceof Map) {
            Object revision = ((Map) doc).get(revisionField);
            Assertions.assertNotNull(revision);
        }
    }

    @Test
    public void testPut() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);

        class TestCase {
            final String name;
            final Object doc;
            final Class<? extends Exception> wantErr;

            TestCase(String name, Object doc, Class<? extends Exception> wantErr) {
                this.name = name;
                this.doc = doc;
                // Treat null as OK.
                this.wantErr = wantErr;
            }
        }

        // Build doc from the user defined class
        List<TestCase> testCases = new ArrayList<>();
        Player player = new Player("JohnPut", 121, 12.66f, true, "randomString");
        testCases.add(new TestCase("PutWithClass", player, null));

        // Key already exists for class
        Player player2 = new Player("JohnPut", 111, 12.66f, true, "randomString2");
        testCases.add(new TestCase("ReplaceAlreadyExistsWithClass", player2, null));

        // Key empty partition key not allowed
        Player player3 = new Player("", 111, 12.66f, true, "randomString2");
        testCases.add(new TestCase("PutWithClassEmptyPartitionKey", player3, InvalidArgumentException.class));

        // Build doc from the map
        Map m = Map.of("pName", "TaylorPut", "i", 121, "f", 12.66f, "b", true, "s", "randomString");
        m = new HashMap(m);
        m.put("DocstoreRevision", null);
        testCases.add(new TestCase("CreateWithMap", m, null));

        // Key already exists for map
        Map m2 = Map.of("pName", "TaylorPut", "i", 121, "f", 12.66f, "b", true, "s", "randomStringMap");
        m2 = new HashMap(m2);
        m2.put("DocstoreRevision", null);
        testCases.add(new TestCase("ReplaceWithMapAlreadyExists", m2, null));

        Map m3 = Map.of("pName", "LeoPut", "i", 121, "f", 12.66f, "b", true, "DocstoreRevision", harness.getRevisionId());
        m3 = new HashMap(m3);
        testCases.add(new TestCase("PutWithNonEmptyRevision", m3, ResourceNotFoundException.class));

        Map m4 = Map.of("pName", "LeoPut", "i", 121, "f", 12.66f, "b", true);
        m4 = new HashMap(m4);
        m4.put("DocstoreRevision", null);
        testCases.add(new TestCase("CreateWithNullRevision", m4, null));

        for (TestCase testCase : testCases) {
            if (testCase.wantErr != null) {
                Assertions.assertThrows(
                        testCase.wantErr,
                        () -> docStoreClient.put(new Document(testCase.doc)),
                        String.format("Test %s failed", testCase.name));
            } else {
                verifyNoRevisionField(testCase.doc, "DocstoreRevision");
                docStoreClient.put(new Document(testCase.doc));
                verifyRevisionFieldExist(testCase.doc, "DocstoreRevision");
                Object got = newDocument(testCase.doc);
                docStoreClient.get(new Document(got));
                if (got instanceof Map) {
                    Assertions.assertTrue(compareMaps((Map) got, (Map) testCase.doc));
                } else {
                    Assertions.assertEquals(testCase.doc, got, String.format("Test %s failed", testCase.name));
                }
            }
        }
        docStoreClient.close();
    }

    @Test
    public void testDelete() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);

        class TestCase {
            final String name;
            final Object doc;
            final Object deleteDoc;
            final Object want;
            final Class<? extends Exception> wantErr;

            TestCase(String name, Object doc, Object deleteDoc, Object want, Class<? extends Exception> wantErr) {
                this.name = name;
                this.doc = doc;
                this.deleteDoc = deleteDoc;
                this.want = want;
                this.wantErr = wantErr;
            }
        }

        List<TestCase> testCases = new ArrayList<>();

        // 1. delete doc of the user-defined class, pass in the complete object
        Player player = new Player("JohnDelete", 121, 12.66f, true, "randomString");
        Player wantPlayer = new Player();
        wantPlayer.setPName("JohnDelete");
        testCases.add(new TestCase("DeleteWithClass", player, player, wantPlayer, null));

        // 2. delete doc of the user-defined class, pass in only partitionKey
        Player player2 = new Player("EdwardDelete", 121, 12.66f, true, "randomString");
        Object deleteDoc2 = newDocument(player2);
        Player wantPlayer2 = new Player();
        wantPlayer2.setPName("EdwardDelete");
        testCases.add(new TestCase("DeleteWithClassPartitionKey", player2, deleteDoc2, wantPlayer2, null));

        // 3. delete object without partition key, throw InvalidArgumentException
        Player player3 = new Player("FrankDelete", 121, 12.66f, true, "randomString");
        Player deletePlayer3 = new Player();
        deletePlayer3.setPName("");
        testCases.add(new TestCase("DeleteWithClassNoPartitionKey", player3, deletePlayer3, null, InvalidArgumentException.class));

        // 4. delete a map pass in the complete map
        Map m1 = Map.of("pName", "TigerMapDelete", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        m1 = new HashMap(m1);
        m1.put("DocstoreRevision", null);
        Map mWant1 = Map.of("pName", "TigerMapDelete");
        testCases.add(new TestCase("DeleteMapPassInCompleteMap", m1, m1, mWant1, null));

        // 5. delete a map pass in the partition key only
        Map m2 = Map.of("pName", "LionMapDelete", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        Object deleteMap2 = Map.of("pName", "LionMapDelete");
        Map mWant2 = Map.of("pName", "LionMapDelete");
        testCases.add(new TestCase("DeleteMapWithOnlyPKey", m2, deleteMap2, mWant2, null));

        // 6. delete a map with revision, pass in the partition key only
        Map m3 = Map.of("pName", "LouisMapDelete", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        m3 = new HashMap(m3);
        m3.put("DocstoreRevision", null);
        Object deleteMap3 = Map.of("pName", "LouisMapDelete");
        Map mWant3 = Map.of("pName", "LouisMapDelete");
        testCases.add(new TestCase("DeleteMapWithRevision", m3, deleteMap3, mWant3, null));

        // 7. delete a map pass in empty partition key, throw InvalidArgumentException
        Map m4 = Map.of("pName", "ClarkMapDelete", "i", 121, "f", 12.66f, "b", true, "s", "random String");
        Object deleteMap4 = Map.of("pName", "");
        testCases.add(new TestCase("DeleteMapWithNoPartitionKey", m4, deleteMap4, null, InvalidArgumentException.class));

        for (TestCase testCase : testCases) {
            docStoreClient.create(new Document(testCase.doc));
            if (testCase.wantErr != null) {
                Assertions.assertThrows(
                        testCase.wantErr,
                        () -> docStoreClient.delete(new Document(testCase.deleteDoc)),
                        String.format("Test %s failed", testCase.name));
            } else {
                docStoreClient.delete(new Document(testCase.deleteDoc));
                // Verify that the document is deleted
                Object got = newDocument(testCase.doc);
                docStoreClient.get(new Document(got));
                Assertions.assertEquals(testCase.want, got, String.format("Test %s failed", testCase.name));
            }
        }
        docStoreClient.close();
    }

    @Test
    public void testReplace() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        class TestCase {
            final String name;
            final Object origDoc;
            final Object replaceDoc;
            final Object wantDoc;
            final Class<? extends Exception> wantErr;

            TestCase(String name, Object origDoc, Object replaceDoc, Object wantDoc, Class<? extends Exception> wantErr) {
                this.name = name;
                this.origDoc = origDoc;
                this.replaceDoc = replaceDoc;
                this.wantDoc = wantDoc;
                // Treat null as OK.
                this.wantErr = wantErr;
            }
        }

        List<TestCase> testCases = new ArrayList<>();

        // 1. Replace all fields for user-defined class
        Player player1 = new Player("JohnReplace", 121, 12.66f, true, "originalString");
        Player replacePlayer1 = new Player("JohnReplace", 95, 34.56f, false, "replacedString");
        testCases.add(new TestCase("ReplaceAllFieldsWithClass", player1, replacePlayer1, replacePlayer1, null));

        // 2. Replace partial fields for user-defined class
        Player player2 = new Player("DavidReplace", 999, 12.66f, true, "originalString");
        Player replacePlayer2 = new Player();
        replacePlayer2.setPName("DavidReplace");
        replacePlayer2.setI(95);
        replacePlayer2.setS("replacedString");
        testCases.add(new TestCase("ReplacePartialFieldsWithClass", player2, replacePlayer2, replacePlayer2, null));

        // 3. Partition Key does not exist for class replacement (simulating replace with non-existing key)
        Player player3 = new Player("EdwardReplace", 999, 12.66f, true, "originalString");
        Player replacePlayer3 = new Player("", 95, 34.56f, false, "replacedString");
        testCases.add(new TestCase("ReplaceNonExistingPKeyWithClass", player3, replacePlayer3, null, InvalidArgumentException.class));

        // 4. Replace non-existent class, should throw ResourceNotFoundException
        Player replacePlayer4 = new Player("NonExistentReplace", 95, 34.56f, false, "replacedString");
        testCases.add(new TestCase("ReplaceNonExistentWithClass", null, replacePlayer4, null, ResourceNotFoundException.class));

        // 5. Replace all fields for map
        Map m1 = Map.of("pName", "TaylorReplace", "i", 121, "f", 12.66f, "b", true, "s", "originalString");
        m1 = new HashMap(m1);
        m1.put("DocstoreRevision", null);
        Map mReplace1 = Map.of("pName", "TaylorReplace", "i", 99, "f", 34.56f, "b", false, "s", "replacedString");
        mReplace1 = new HashMap(mReplace1);
        mReplace1.put("DocstoreRevision", null);
        testCases.add(new TestCase("ReplaceAllFieldsWithMap", m1, mReplace1, mReplace1, null));

        // 6. Replace partial fields for map
        Map m2 = Map.of("pName", "KingReplace", "i", 121, "f", 12.66f, "b", true, "s", "originalString");
        m2 = new HashMap(m2);
        m2.put("DocstoreRevision", null);
        Map mReplace2 = Map.of("pName", "KingReplace", "f", 34.56f, "s", "replacedString");
        mReplace2 = new HashMap(mReplace2);
        mReplace2.put("DocstoreRevision", null);
        testCases.add(new TestCase("ReplacePartialFieldsWithMap", m2, mReplace2, mReplace2, null));

        // 7. Partition Key does not exist for map replacement (simulating replace with non-existing key)
        Map m3 = Map.of("pName", "LeoReplace", "i", 121, "f", 12.66f, "b", true, "s", "originalString");
        m3 = new HashMap(m3);
        m3.put("DocstoreRevision", null);
        Map mReplace3 = Map.of("pName", "", "i", 99, "f", 12.34f, "b", false, "s", "replacedString");
        mReplace3 = new HashMap(mReplace3);
        mReplace3.put("DocstoreRevision", null);
        testCases.add(new TestCase("ReplaceMissingPKeyWithMap", m3, mReplace3, null, InvalidArgumentException.class));

        // 8. Replace a non-existing map, throw ResourceNotFoundException
        Map mReplace4 = Map.of("pName", "NonExistentMapReplace", "i", 99, "f", 12.34f, "b", false, "s", "replacedString");
        mReplace4 = new HashMap(mReplace4);
        mReplace4.put("DocstoreRevision", null);
        testCases.add(new TestCase("ReplaceNonExistentWithMap", null, mReplace4, null, ResourceNotFoundException.class));

        for (TestCase testCase : testCases) {
            if (testCase.origDoc != null) {
                verifyNoRevisionField(testCase.origDoc, "DocstoreRevision");
                docStoreClient.create(new Document(testCase.origDoc));
                verifyRevisionFieldExist(testCase.origDoc, "DocstoreRevision");
            }
            if (testCase.wantErr != null) {
                Assertions.assertThrows(
                        testCase.wantErr,
                        () -> docStoreClient.replace(new Document(testCase.replaceDoc)),
                        String.format("Test %s failed", testCase.name));
            } else {
                verifyNoRevisionField(testCase.replaceDoc, "DocstoreRevision");
                docStoreClient.replace(new Document(testCase.replaceDoc));
                verifyRevisionFieldExist(testCase.replaceDoc, "DocstoreRevision");
                Object got = newDocument(testCase.origDoc);
                docStoreClient.get(new Document(got));
                if (got instanceof Map) {
                    Assertions.assertTrue(compareMaps((Map) got, (Map) testCase.wantDoc));
                } else {
                    Assertions.assertEquals(testCase.wantDoc, got, String.format("Test %s failed", testCase.name));
                }
            }
        }
        docStoreClient.close();
    }

    @Test
    public void testGetQuery() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.TWO_KEYS);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        // Create test data
        List<HighScore> allScores = List.of(
                new HighScore(game1, "pat", 49, "2024-03-13", false),
                new HighScore(game1, "mel", 60, "2024-04-10", false),
                new HighScore(game1, "andy", 81, "2024-02-01", false),
                new HighScore(game1, "fran", 33, "2024-03-19", false),
                new HighScore(game2, "pat", 120, "2024-04-01", true),
                new HighScore(game2, "billie", 111, "2024-04-10", false),
                new HighScore(game2, "mel", 190, "2024-04-18", true),
                new HighScore(game2, "fran", 33, "2024-03-20", false)
        );
        for (HighScore score : allScores) {
            docStoreClient.put(new Document(score));
        }

        // 1. Query all
        DocumentIterator iter = docStoreClient.query().get();
        List<HighScore> actualScores = getQueryResult(iter);
        Assertions.assertEquals(8, actualScores.size());
        Assertions.assertTrue(allScores.containsAll(actualScores));

        // 2. Query a Game
        DocumentIterator iter2 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game2).get();
        List<HighScore> game2Scores = getQueryResult(iter2);
        Assertions.assertEquals(4, game2Scores.size());
        Assertions.assertTrue(game2Scores.containsAll(allScores.stream().filter(s -> s.Game.equals(game2)).collect(Collectors.toList())));

        // 3. Query score greater than 100
        DocumentIterator iter3 = docStoreClient.query().where("Score", FilterOperation.GREATER_THAN, 100).get();
        List<HighScore> greatThan100Scores = getQueryResult(iter3);
        Assertions.assertEquals(3, greatThan100Scores.size());
        Assertions.assertTrue(greatThan100Scores.containsAll(allScores.stream().filter(s -> s.Score > 100).collect(Collectors.toList())));

        // 4. query a player
        DocumentIterator iter4 = docStoreClient.query().where("Player", FilterOperation.EQUAL, "billie").get();
        List<HighScore> billieScores = getQueryResult(iter4);
        Assertions.assertEquals(1, billieScores.size());
        Assertions.assertTrue(billieScores.containsAll(allScores.stream().filter(s -> s.Player.equals("billie")).collect(Collectors.toList())));

        // 5. query game player
        DocumentIterator iter5 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game1).where("Player", FilterOperation.EQUAL, "andy").get();
        List<HighScore> gamePlayerScores = getQueryResult(iter5);
        Assertions.assertEquals(1, gamePlayerScores.size());
        Assertions.assertTrue(gamePlayerScores.containsAll(allScores.stream().filter(s -> s.Game.equals(game1) && s.Player.equals("andy")).collect(Collectors.toList())));

        // 6. query player score
        DocumentIterator iter6 = docStoreClient.query().where("Player", FilterOperation.EQUAL, "pat").where("Score", FilterOperation.LESS_THAN, 100).get();
        List<HighScore> playerScores = getQueryResult(iter6);
        Assertions.assertEquals(1, playerScores.size());
        Assertions.assertTrue(playerScores.containsAll(allScores.stream().filter(s -> s.Player.equals("pat") && s.Score < 100).collect(Collectors.toList())));

        // 7. query game score
        DocumentIterator iter7 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game1).where("Score", FilterOperation.GREATER_THAN_OR_EQUAL_TO, 50).get();
        List<HighScore> gameScores = getQueryResult(iter7);
        Assertions.assertEquals(2, gameScores.size());
        Assertions.assertTrue(gameScores.containsAll(allScores.stream().filter(s -> s.Game.equals(game1) && s.Score >= 50).collect(Collectors.toList())));

        // 8. query player in
        DocumentIterator iter8 = docStoreClient.query().where("Player", FilterOperation.IN, List.of("pat", "billie")).get();
        List<HighScore> playerIns = getQueryResult(iter8);
        Assertions.assertEquals(3, playerIns.size());
        Assertions.assertTrue(playerIns.containsAll(allScores.stream().filter(s -> List.of("pat", "billie").contains(s.Player)).collect(Collectors.toList())));

        // 9. query player not in
        DocumentIterator iter9 = docStoreClient.query().where("Player", FilterOperation.NOT_IN, List.of("pat", "billie")).get();
        List<HighScore> playerNotIns = getQueryResult(iter9);
        Assertions.assertEquals(5, playerNotIns.size());
        Assertions.assertTrue(playerNotIns.containsAll(allScores.stream().filter(s -> !List.of("pat", "billie").contains(s.Player)).collect(Collectors.toList())));

        // 10. query boolean equals
        DocumentIterator iter10 = docStoreClient.query().where("WithGlitch", FilterOperation.EQUAL, true).get();
        List<HighScore> withGlitches = getQueryResult(iter10);
        Assertions.assertEquals(2, withGlitches.size());

        // 11. query boolean In
        DocumentIterator iter11 = docStoreClient.query().where("WithGlitch", FilterOperation.IN, List.of(true)).get();
        List<HighScore> withGlitchIns = getQueryResult(iter11);
        Assertions.assertEquals(2, withGlitchIns.size());

        // 12. query boolean Not In
        DocumentIterator iter12 = docStoreClient.query().where("WithGlitch", FilterOperation.NOT_IN, List.of(true)).get();
        List<HighScore> withGlitchNotIns = getQueryResult(iter12);
        Assertions.assertEquals(6, withGlitchNotIns.size());

        // 13. query all order by player (SK) asc, should fail because full scans with order by are not allowe
        if (harness.supportOrderByInFullScan()) {
            Assertions.assertDoesNotThrow(() -> docStoreClient.query().orderBy("Player", true).get()
            );
        } else {
            Assertions.assertThrows(
                    InvalidArgumentException.class,
                    () -> docStoreClient.query().orderBy("Player", true).get()
            );
        }


        // 14. query all order by player desc, should fail because full scans with order by are not allowed
        if (harness.supportOrderByInFullScan()) {
            Assertions.assertDoesNotThrow(() -> docStoreClient.query().orderBy("Player", false).get()
            );
        } else {
            Assertions.assertThrows(
                    InvalidArgumentException.class,
                    () -> docStoreClient.query().orderBy("Player", false).get()
            );
        }

        // 15. Test for valid order by clause.
        DocumentIterator iter15 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game1)
                .where("Player", FilterOperation.GREATER_THAN, ".")
                .orderBy("Player", true).get();
        List<HighScore> gamePlayerAsc = getQueryResult(iter15);
        Assertions.assertEquals(4, gamePlayerAsc.size());

        // 16. Test query table with pagination token. Result contains 1 full page and 1 half page.
        DocumentIterator iter16 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game2)
                .where("Player", FilterOperation.GREATER_THAN, "billie")
                .orderBy("Player", true).limit(2).get();
        List<HighScore> gamePlayerScore = getQueryResult(iter16);
        Assertions.assertEquals(2, gamePlayerScore.size());
        PaginationToken paginationToken = iter16.getPaginationToken();
        Assertions.assertFalse(paginationToken.isEmpty());

        iter16 = docStoreClient.query()
                .where("Game", FilterOperation.EQUAL, game2)
                .where("Player", FilterOperation.GREATER_THAN, "billie")
                .orderBy("Player", true).paginationToken(paginationToken).limit(2).get();
        gamePlayerScore = getQueryResult(iter16);
        Assertions.assertEquals(1, gamePlayerScore.size());
        Assertions.assertEquals("pat", gamePlayerScore.get(0).Player);

        // 17. Test query table with pagination token. Result contains only 1 page.
        DocumentIterator iter17 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game1)
                .where("Player", FilterOperation.GREATER_THAN, "andy").
                orderBy("Player", true).offset(1).limit(2).get();
        List<HighScore> highScores = getQueryResult(iter17);
        Assertions.assertEquals(2, highScores.size());
        Assertions.assertEquals("mel", highScores.get(0).Player);

        // 18. Test scan table with pagination token. Result contains 2 full pages.
        DocumentIterator iter18 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game2)
                .where("WithGlitch", FilterOperation.EQUAL, false).offset(0).limit(1).get();
        highScores = getQueryResult(iter18);
        Assertions.assertEquals(1, highScores.size());
        paginationToken = iter18.getPaginationToken();
        Assertions.assertFalse(paginationToken.isEmpty());

        iter18 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game2)
                .where("WithGlitch", FilterOperation.EQUAL, false).paginationToken(paginationToken).limit(1).get();
        highScores = getQueryResult(iter18);
        Assertions.assertEquals(1, highScores.size());
        Assertions.assertEquals("fran", highScores.get(0).Player);

        // 19. Test query from local index with pagination token. Result contains 1 full page and 1 half page.
        DocumentIterator iter19 = docStoreClient.query().where("Game", FilterOperation.EQUAL, game2)
                .where("Score", FilterOperation.GREATER_THAN, 100).orderBy("Score", true).limit(2).get();
        highScores = getQueryResult(iter19);
        Assertions.assertEquals(2, highScores.size());
        paginationToken = iter19.getPaginationToken();
        Assertions.assertFalse(paginationToken.isEmpty());

        iter19 = docStoreClient.query()
                .where("Game", FilterOperation.EQUAL, game2)
                .where("Score", FilterOperation.GREATER_THAN, 100)
                .orderBy("Score", true).paginationToken(paginationToken).limit(2).get();
        highScores = getQueryResult(iter19);
        Assertions.assertEquals(1, highScores.size());
        Assertions.assertEquals("mel", highScores.get(0).Player);

        // 20. Test query from global index with pagination token. Result contains 2 full pages.
        DocumentIterator iter20 = docStoreClient.query().where("Player", FilterOperation.EQUAL, "mel")
                .where("Time", FilterOperation.GREATER_THAN, "2024-02-01").orderBy("Time", true).limit(1).get();
        highScores = getQueryResult(iter20);
        Assertions.assertEquals(1, highScores.size());
        paginationToken = iter20.getPaginationToken();
        Assertions.assertFalse(paginationToken.isEmpty());

        iter20 = docStoreClient.query()
                .where("Player", FilterOperation.EQUAL, "mel")
                .where("Time", FilterOperation.GREATER_THAN, "2024-02-01").orderBy("Time", true).paginationToken(paginationToken).limit(1).get();
        highScores = getQueryResult(iter20);
        Assertions.assertEquals(1, highScores.size());
        Assertions.assertEquals("2024-04-18", highScores.get(0).Time);
        
        docStoreClient.close();
    }

    private static List<HighScore> getQueryResult(DocumentIterator iter) {
        List<HighScore> actualScores = new ArrayList<>();
        while (iter.hasNext()) {
            HighScore score = new HighScore();
            iter.next(new Document(score));
            actualScores.add(score);
        }
        return actualScores;
    }

    @AllArgsConstructor
    @Data
    @NoArgsConstructor
    private static class HighScore {
        private String Game;
        private String Player;
        private int Score;
        private String Time;
        private boolean WithGlitch;
    }

    private static String game1 = "Praise All Monsters";
    private static String game2 = "Zombie DMV";
    private static String game3 = "Days Gone";

    @Test
    public void testAtomicWrites() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        String revField = "DocstoreRevision";
        
        // Create 9 test documents following the Go pattern
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("pName", String.format("testAtomicWrites%d", i));
            doc.put("s", String.valueOf(i));
            doc.put("i", i);
            //doc.put("f", (float) (i+.10*i));
            doc.put("b", i % 2 == 0);
            doc.put(revField, null);
            docs.add(doc);
        }

        // Put the nine docs
        ActionList actions = docStoreClient.getActions();
        for (int i = 0; i < 9; i++) {
            actions.create(new Document(docs.get(i)));
        }
        actions.run();

        // Verify all documents were created and have revision fields
        for (Map<String, Object> doc : docs) {
            verifyRevisionFieldExist(doc, revField);
        }

        // Delete the first three, get the second three, and update last three atomically
        // Prepare get documents
        List<Map<String, Object>> getDocs = new ArrayList<>();
        for (int i = 3; i < 6; i++) {
            Map<String, Object> getDoc = new HashMap<>();
            getDoc.put("pName", docs.get(i).get("pName"));
            getDocs.add(getDoc);
        }

        actions = docStoreClient.getActions();
        // Get operations
        actions.get(new Document(getDocs.get(0)));
        // Delete operations
        actions.delete(new Document(docs.get(0)));
        actions.delete(new Document(docs.get(1)));
        actions.get(new Document(getDocs.get(1)));
        actions.delete(new Document(docs.get(2)));
        actions.get(new Document(getDocs.get(2)));
        
        // Enable atomic writes for the following operations
        actions.enableAtomicWrites();
        Map t6 = docs.get(6);
        t6.put("s", "66");
        actions.put(new Document(t6));
        Map t7 = docs.get(7);
        t7.put("s", "77");
        actions.put(new Document(t7));
        Map t8 = docs.get(8);
        t8.put("s", "88");
        actions.put(new Document(t8));
        actions.run();

        // Verify get documents contain the expected data
        for (int i = 0; i < 3; i++) {
            Map<String, Object> expected = docs.get(i + 3);
            Map<String, Object> actual = getDocs.get(i);
            // The get operation should have populated these documents
            Assertions.assertEquals(expected.get("pName"), actual.get("pName"));
            Assertions.assertEquals(expected.get("s"), actual.get("s"));
            Assertions.assertEquals(expected.get("i"), actual.get("i"));
            //Assertions.assertEquals(expected.get("f"), actual.get("f"));
            Assertions.assertEquals(expected.get("b"), actual.get("b"));
            Assertions.assertNotNull(actual.get(revField));
        }

        // Get the docs updated as part of atomic writes and verify values were updated successfully
        Map<String, Object> doc6 = new HashMap<>();
        doc6.put("pName", docs.get(6).get("pName"));
        docStoreClient.get(new Document(doc6));
        Assertions.assertEquals("66", doc6.get("s"));

        Map<String, Object> doc7 = new HashMap<>();
        doc7.put("pName", docs.get(7).get("pName"));
        docStoreClient.get(new Document(doc7));
        Assertions.assertEquals("77", doc7.get("s"));

        Map<String, Object> doc8 = new HashMap<>();
        doc8.put("pName", docs.get(8).get("pName"));
        docStoreClient.get(new Document(doc8));
        Assertions.assertEquals("88", doc8.get("s"));

        docStoreClient.close();
    }

    @Test
    public void testAtomicWritesFail() {
        AbstractDocStore docStore = harness.createDocstoreDriver(CollectionKind.SINGLE_KEY);
        DocStoreClient docStoreClient = new DocStoreClient(docStore);
        String revField = "DocstoreRevision";
        
        // Create 9 test documents but only put the first 8 (doc8 won't exist)
        List<Map<String, Object>> docs = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("pName", String.format("testAtomicWritesFail%d", i));
            doc.put("s", String.valueOf(i));
            doc.put("i", i);
            doc.put("f", (float) (i+.10*i));
            doc.put("b", i % 2 == 0);
            doc.put(revField, null);
            docs.add(doc);
        }

        // Put only the first eight docs (docs[8] doesn't exist)
        ActionList actions = docStoreClient.getActions();
        for (int i = 0; i < 8; i++) {
            actions.create(new Document(docs.get(i)));
        }
        actions.run();

        // Verify first 8 documents were created and have revision fields
        for (int i = 0; i < 8; i++) {
            verifyRevisionFieldExist(docs.get(i), revField);
        }

        // Delete the first three, get the second three, and update last three atomically
        // Prepare get documents
        List<Map<String, Object>> getDocs = new ArrayList<>();
        for (int i = 3; i < 6; i++) {
            Map<String, Object> getDoc = new HashMap<>();
            getDoc.put("pName", docs.get(i).get("pName"));
            getDocs.add(getDoc);
        }

        actions = docStoreClient.getActions();
        // Get operations
        actions.get(new Document(getDocs.get(0)));
        // Delete operations
        actions.delete(new Document(docs.get(0)));
        actions.delete(new Document(docs.get(1)));
        actions.get(new Document(getDocs.get(1)));
        actions.delete(new Document(docs.get(2)));
        actions.get(new Document(getDocs.get(2)));

        // Atomic writes operations - the last one will fail because docs[8] doesn't exist
        actions.enableAtomicWrites();
        Map t6 = docs.get(6);
        t6.put("s", "66");
        actions.put(new Document(t6));
        Map t7 = docs.get(7);
        t7.put("s", "77");
        actions.put(new Document(t7));
        Map t8 = docs.get(8);
        t8.put(revField, "88");
        actions.put(new Document(t8));

        // The atomic transaction should fail
        Assertions.assertThrows(TransactionFailedException.class, actions::run);

        // Verify get documents still contain the expected data (from before the failed transaction)
        for (int i = 0; i < 3; i++) {
            Map<String, Object> expected = docs.get(i + 3);
            Map<String, Object> actual = getDocs.get(i);
            // The get operation should have populated these documents
            Assertions.assertEquals(expected.get("pName"), actual.get("pName"));
            Assertions.assertEquals(expected.get("s"), actual.get("s"));
            Assertions.assertEquals(expected.get("i"), actual.get("i"));
            Assertions.assertEquals(expected.get("f"), actual.get("f"));
            Assertions.assertEquals(expected.get("b"), actual.get("b"));
            Assertions.assertNotNull(actual.get(revField));
        }

        // Validate that the values still remain the original (atomic rollback)
        Map<String, Object> doc6 = new HashMap<>();
        doc6.put("pName", docs.get(6).get("pName"));
        docStoreClient.get(new Document(doc6));
        Assertions.assertEquals("6", doc6.get("s")); // Should still be original value

        Map<String, Object> doc7 = new HashMap<>();
        doc7.put("pName", docs.get(7).get("pName"));
        docStoreClient.get(new Document(doc7));
        Assertions.assertEquals("7", doc7.get("s")); // Should still be original value

        docStoreClient.close();
    }
}