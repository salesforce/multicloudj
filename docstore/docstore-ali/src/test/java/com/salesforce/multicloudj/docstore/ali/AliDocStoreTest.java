package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CommitTransactionRequest;
import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.ActionList;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.BookWithoutNest;
import com.salesforce.multicloudj.sts.model.CredentialsOverrider;
import com.salesforce.multicloudj.sts.model.CredentialsType;
import com.salesforce.multicloudj.sts.model.StsCredentials;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AliDocStoreTest {
    BookWithoutNest book = new BookWithoutNest("YellowBook", "Neil", "WA", Timestamp.newBuilder().setNanos(1000).build(), 3.99f, null);
    TestDocStore docStore;
    SyncClient syncClient;
    IndexMeta localIndex1;
    IndexMeta localIndex2;
    IndexMeta globalIndex1;

    static class TestDocStore extends AliDocStore {
        @Override
        public void close() {

        }
    }

    static class TestAction extends Action {
        public TestAction(ActionKind kind, Document document, List<String> fieldPaths, Map<String, Object> mods) {
            super(kind, document, fieldPaths, mods, false);
        }

        public void setKey(Object key) {
            this.key = key;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

    @AfterEach
    void testDown() {
        if (ali != null) {
            ali.close();
        }
    }

    private AliDocStore ali;

    @BeforeEach
    void setup() {
        docStore = new TestDocStore();
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .withMaxOutstandingActionRPCs(10)
                .build();
        try {
            Field field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        syncClient = mock(SyncClient.class);
        BatchGetRowResponse response = mock(BatchGetRowResponse.class);
        when(response.getBatchGetRowResult(any())).thenReturn(List.of());
        when(syncClient.putRow(any())).thenReturn(null);
        when(syncClient.batchGetRow(any())).thenReturn(response);
        when(syncClient.sqlQuery(any())).thenReturn(null);
        StartLocalTransactionResponse txResponse = mock(StartLocalTransactionResponse.class);
        when(txResponse.getTransactionID()).thenReturn("tx-id");
        when(syncClient.startLocalTransaction(any())).thenReturn(txResponse);
        when(syncClient.commitTransaction(new CommitTransactionRequest("tx-id"))).thenReturn(null);
        StsCredentials stsCredentials = new StsCredentials("key-1", "secret-1", "token-1");
        CredentialsOverrider credsOverrider = new CredentialsOverrider.Builder(CredentialsType.SESSION)
                .withSessionCredentials(stsCredentials).build();

        SQLQueryResponse mockQueryResponse = mock(SQLQueryResponse.class);
        when(mockQueryResponse.getSQLResultSet()).thenReturn(new TestSQLResultSet());
        when(mockQueryResponse.getNextSearchToken()).thenReturn("testToken");
        when(syncClient.sqlQuery(any(SQLQueryRequest.class))).thenReturn(mockQueryResponse);

        ali = new AliDocStore.Builder().withRegion("cn-shanghai")
                .withEndpointType("internet")
                .withInstanceId("something")
                .withCollectionOptions(
                        new CollectionOptions.CollectionOptionsBuilder()
                                .withPartitionKey("title").withSortKey("publisher")
                                .withTableName("my-table")
                                .withRevisionField("docRevision").build()
                ).withCredentialsOverrider(credsOverrider).withTableStoreClient(syncClient).build();

        localIndex1 = new IndexMeta("local_index_1");
        localIndex1.setIndexType(IndexType.IT_LOCAL_INDEX);
        localIndex1.addPrimaryKeyColumn("title");
        localIndex1.addPrimaryKeyColumn("price");
        localIndex1.addDefinedColumn("author");

        localIndex2 = new IndexMeta("local_index_2");
        localIndex2.addPrimaryKeyColumn("title");
        localIndex2.addPrimaryKeyColumn("author");
        localIndex2.setIndexType(IndexType.IT_LOCAL_INDEX);

        globalIndex1 = new IndexMeta("global_index_3");
        globalIndex1.setIndexType(IndexType.IT_GLOBAL_INDEX);
        globalIndex1.addPrimaryKeyColumn("author");
        globalIndex1.addPrimaryKeyColumn("price");

        DescribeTableResponse mockDescribeTableResponse = new DescribeTableResponse(new Response());
        mockDescribeTableResponse.setTableMeta(new TableMeta("my-table"));
        mockDescribeTableResponse.addIndexMeta(localIndex1);
        mockDescribeTableResponse.addIndexMeta(localIndex2);
        mockDescribeTableResponse.addIndexMeta(globalIndex1);

        when(syncClient.describeTable(any(DescribeTableRequest.class))).thenReturn(mockDescribeTableResponse);
    }

    @AfterEach
    void tearDown() {
        ali.close();
    }

    @Test
    void testDefaultConstructor() {
        AliDocStore docStore = new AliDocStore();
        Assertions.assertNotNull(docStore);

        AliDocStore.Builder builder = docStore.builder();
        Assertions.assertNotNull(builder);
    }

    @Test
    void testGetException() {
        AliDocStore docStore = new AliDocStore();
        Class<?> clazz = docStore.getException(new ClientException("hello", ""));
        Assertions.assertEquals(InvalidArgumentException.class, clazz);

        clazz = docStore.getException(new TableStoreException("does-not-matter", "OTSAuthFailed"));
        Assertions.assertEquals(UnAuthorizedException.class, clazz);
    }

    @Test
    void testCreateDocStoreWithEndpoint() {
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("chameleon-ali-test")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .build();

        ali = new AliDocStore.Builder()
                .withRegion("cn-shanghai")
                .withCollectionOptions(collectionOptions)
                .withEndpoint(URI.create("http://your-uri"))
                .withInstanceId("something")
                .build();

        Assertions.assertNotNull(ali);
    }

    @Test
    void testProviderId() {
        Assertions.assertEquals("ali", ali.getProviderId());
    }

    @Test
    void testNewPutWithMissingKey() {
        TestAction replace = new TestAction(ActionKind.ACTION_KIND_REPLACE, new Document(book), null, null);
        Assertions.assertEquals(ActionKind.ACTION_KIND_REPLACE, docStore.newWriteOperation(replace, null).getAction().getKind());

        TestAction update = new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(book), null, null);
        Assertions.assertNull(docStore.newWriteOperation(update, null));

        TestAction delete = new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);
        Assertions.assertEquals(ActionKind.ACTION_KIND_DELETE, docStore.newWriteOperation(delete, null).getAction().getKind());
    }

    @Test
    void testNewPutWithCreate() {
        TestAction create = new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);
        Assertions.assertDoesNotThrow(() -> docStore.newPut(create, null));
    }

    @Test
    void testEncodeObjectWithFields() {
        @Getter
        @AllArgsConstructor
        class Person {
            private String name;
            private int age;
        }
        Person person = new Person("Kate", 23);
        Document doc = new Document(person);
        AliEncoder encoder = new AliEncoder();
        doc.encode(encoder);
        Map<String, ColumnValue> av = encoder.getMap();
        Assertions.assertEquals(2, av.size());
    }

    @Test
    void testGetKey() {
        Document document = new Document(book);
        Assertions.assertEquals("partitionKey:YellowBook,sortKey:WA", ali.getKey(document));
    }

    @Test
    void testGet() {
        Book bookObj = new Book("YellowBook", null, "WA", null, 3.99f, null, null);
        Document document = new Document(bookObj);
        ali.getActions().get(document, "price").run();
    }

    @Test
    void testWritesTx() {
        Book bookObj = new Book("YellowBook", null, "WA", null, 3.99f, null, null);
        Book bookOb2 = new Book("YellowBook", null, "PA", null, 3.99f, null, null);
        Book bookObjTx1 = new Book("BlueBook", null, "WA", null, 3.99f, null, null);
        Book bookObjTx2 = new Book("BlueBook", null, "TX", null, 3.99f, null, null);
        Book bookObjTx3 = new Book("BlueBook", null, "CA", null, 3.99f, null, null);
        ActionList writes = ali.getActions()
                .create(new Document(bookObj))
                .create(new Document(bookOb2))
                .enableAtomicWrites()
                .create(new Document(bookObjTx1))
                .create(new Document(bookObjTx2))
                .create(new Document(bookObjTx3));
        writes.run();

        ArgumentCaptor<PutRowRequest> argumentCaptorPutRow = ArgumentCaptor.forClass(PutRowRequest.class);
        verify(syncClient, times(5)).putRow(argumentCaptorPutRow.capture());

        int count = 0;
        for (PutRowRequest request : argumentCaptorPutRow.getAllValues()) {
            if (request.getRowChange().getPrimaryKey().getPrimaryKeyColumn("title").getValue().asString().equals("BlueBook")) {
                Assertions.assertEquals("tx-id", request.getTransactionId());
                count++;
            }
        }

        Assertions.assertEquals(3, count, "Number of puts in transaction doesn't match");
        ArgumentCaptor<CommitTransactionRequest> argumentCaptorTx = ArgumentCaptor.forClass(CommitTransactionRequest.class);
        verify(syncClient, times(1)).commitTransaction(argumentCaptorTx.capture());
        Assertions.assertEquals("tx-id", argumentCaptorTx.getValue().getTransactionID());
    }

    @Test
    void testEmptyActions() {
        ActionList writes = ali.getActions();
        Assertions.assertDoesNotThrow(writes::run);

    }

    @Test
    void testRunGetsWithException() {
        TestDocStore docStore = new TestDocStore();
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .withMaxOutstandingActionRPCs(10)
                .build();

        List<String> fp1 = new ArrayList<>(List.of("title", "publisher"));
        List<String> fp2 = new ArrayList<>(List.of("publisher", "title"));  // Different order of fp1, treated as a different fp.
        List<String> fp3 = new ArrayList<>(List.of("title"));
        List<String> fp4 = new ArrayList<>(List.of("publisher"));

        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
        List<Action> gets = new ArrayList<>();

        TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
        get1.setKey("partitionKey:YellowBook,sortKey:WA");
        TestAction get2 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp2, null);
        get2.setKey("partitionKey:YellowBook,sortKey:WA");
        TestAction get3 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp3, null);
        get3.setKey("partitionKey:YellowBook,sortKey:WA");
        TestAction get4 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp4, null);
        get4.setKey("partitionKey:YellowBook,sortKey:WA");

        gets.add(get1);
        gets.add(get2);
        gets.add(get3);
        gets.add(get4);

        try {
            Field field = docStore.getClass().getSuperclass().getDeclaredField("batchSize");
            field.setAccessible(true);
            field.set(docStore, 2);

            field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.", e);
        }
        Assertions.assertThrows(ExecutionException.class, () -> docStore.runGets(gets, null, 1));
    }

    @Test
    void testRunGets() {
        TestDocStore docStore = new TestDocStore();
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withMaxOutstandingActionRPCs(10)
                .build();

        List<String> fp1 = new ArrayList<>(List.of("title", "publisher"));
        List<String> fp2 = new ArrayList<>(List.of("publisher", "title"));  // Different order of fp1, treated as a different fp.

        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
        List<Action> gets = new ArrayList<>();

        TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
        get1.setKey("partitionKey:YellowBook,sortKey:WA");
        TestAction get2 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp2, null);
        get2.setKey("partitionKey:YellowBook,sortKey:WA");

        gets.add(get1);
        gets.add(get2);

        try {
            Field field = docStore.getClass().getSuperclass().getDeclaredField("batchSize");
            field.setAccessible(true);
            field.set(docStore, 1);

            field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.", e);
        }

        SyncClient tsClient = mock(SyncClient.class);
        try {
            Field field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(docStore, tsClient);

        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        BatchGetRowResponse mockResponse = mock(BatchGetRowResponse.class);
        when(tsClient.batchGetRow(any())).thenReturn(mockResponse);
        List<BatchGetRowResponse.RowResult> responses = new ArrayList<>();
        Column title = new Column("title", ColumnValue.fromString("YellowBook"));
        Column publisher = new Column("publisher", ColumnValue.fromString("WA"));
        Column author = new Column("author", ColumnValue.fromString("Jamie"));

        BatchGetRowResponse.RowResult item = new BatchGetRowResponse.RowResult("table",
                new Row(PrimaryKeyBuilder.createPrimaryKeyBuilder().build(), List.of(title, publisher, author)),
                null, 0);

        responses.add(0, item);

        when(mockResponse.getBatchGetRowResult("table")).thenReturn(responses);
        Assertions.assertDoesNotThrow(() -> docStore.runGets(gets, null, 1));
    }

    @Test
    void testBatchGetWithMissingKeys() {
        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
        List<Action> gets = new ArrayList<>();
        List<String> fp1 = new ArrayList<>(List.of("d"));
        TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
        gets.add(get1);

        TestDocStore docStore = new TestDocStore();
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withMaxOutstandingActionRPCs(10)
                .build();
        try {
            Field field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);
            field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(docStore, syncClient);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        // Test when the key objects are missing.
        Assertions.assertThrows(NullPointerException.class, () -> docStore.batchGet(gets, null, 0, 0));
    }

    @Test
    void testBatchGet() {
        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
        List<Action> gets = new ArrayList<>();
        List<String> fp1 = new ArrayList<>(List.of("title", "publisher", "author"));
        TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
        get1.setKey("partitionKey:YellowBook,sortKey:WA");
        gets.add(get1);

        TestDocStore docStore = new TestDocStore();
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withMaxOutstandingActionRPCs(10)
                .build();

        SyncClient tsClient = mock(SyncClient.class);
        try {
            Field field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);

            field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(docStore, tsClient);

        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        BatchGetRowResponse mockResponse = mock(BatchGetRowResponse.class);
        when(tsClient.batchGetRow(any())).thenReturn(mockResponse);
        List<BatchGetRowResponse.RowResult> responses = new ArrayList<>();
        Column title = new Column("title", ColumnValue.fromString("YellowBook"));
        Column publisher = new Column("publisher", ColumnValue.fromString("WA"));
        Column author = new Column("author", ColumnValue.fromString("Jamie"));

        BatchGetRowResponse.RowResult item = new BatchGetRowResponse.RowResult("table",
                new Row(PrimaryKeyBuilder.createPrimaryKeyBuilder().build(), List.of(title, publisher, author)),
                null, 0);

        responses.add(0, item);

        when(mockResponse.getBatchGetRowResult("table")).thenReturn(responses);
        Assertions.assertNull(gets.get(0).getDocument().getField("author"));
        Assertions.assertDoesNotThrow(() -> docStore.batchGet(gets, null, 0, 0));
        Assertions.assertEquals("Jamie", gets.get(0).getDocument().getField("author"));
    }

    @Test
    void testCreate() {
        Document document = new Document(book);
        Assertions.assertDoesNotThrow(() -> ali.getActions().create(document).run());
    }

    @Test
    void testPut() {
        Document document = new Document(book);
        Assertions.assertDoesNotThrow(() -> ali.getActions().put(document).run());
    }

    @Test
    void testPutWithRevision() {
        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, "something");
        Document document = new Document(bookObj);
        Assertions.assertDoesNotThrow(() -> ali.getActions().put(document).run());
    }

    @Test
    void testDeleteWithRevision() {
        BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, "something");
        Document document = new Document(bookObj);
        Assertions.assertDoesNotThrow(() -> ali.getActions().delete(document).run());
    }

    @Test
    void testExceptionHandling() {
        TableStoreException tsException = new TableStoreException("test1", "OTSNoPermissionAccess");
        Class<?> cls = ali.getException(tsException);
        Assertions.assertEquals(cls, UnAuthorizedException.class);

        ClientException clientException = new ClientException();
        cls = ali.getException(clientException);
        Assertions.assertEquals(cls, InvalidArgumentException.class);
    }

    @Test
    void testRunPut() {
        TestDocStore docStore = new TestDocStore();
        SyncClient tsClient = mock(SyncClient.class);
        CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("table")
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withAllowScans(false)
                .withRevisionField("docRevision")
                .withMaxOutstandingActionRPCs(10)
                .build();
        try {
            Field field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
            field.setAccessible(true);
            field.set(docStore, collectionOptions);

            field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(docStore, tsClient);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        Map<String, ColumnValue> av = AliCodec.encodeDoc(new Document(book));
        TestAction create = new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);

        RowPutChange put = new RowPutChange("table", PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("title", PrimaryKeyValue.fromColumn(av.get("title")))
                .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromColumn(av.get("publisher"))).build());

        PutRowResponse mockResponse = mock(PutRowResponse.class);
        when(tsClient.putRow(any())).thenReturn(mockResponse);
        Assertions.assertDoesNotThrow(() -> docStore.runPut(put, create, null));
    }

    @Test
    void testNewUpdate() {
        try (TestDocStore docStore = new TestDocStore()) {
            Assertions.assertDoesNotThrow(() -> docStore.newUpdate(null, null));
        }
    }

    @Test
    void testRunDelete() {
        try (TestDocStore docStore = new TestDocStore()) {
            CollectionOptions collectionOptions = new CollectionOptions.CollectionOptionsBuilder()
                    .withTableName("table")
                    .withPartitionKey("title")
                    .withSortKey("publisher")
                    .withAllowScans(false)
                    .withMaxOutstandingActionRPCs(10)
                    .build();
            BookWithoutNest bookObj = new BookWithoutNest("YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
            List<Action> deletes = new ArrayList<>();

            TestAction delete1 = new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(bookObj), null, null);
            delete1.setKey("partitionKey:YellowBook,sortKey:WA");

            deletes.add(delete1);

            try {
                Field field = docStore.getClass().getSuperclass().getDeclaredField("batchSize");
                field.setAccessible(true);
                field.set(docStore, 1);

                field = docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
                field.setAccessible(true);
                field.set(docStore, collectionOptions);
            } catch (Exception e) {
                Assertions.fail("Failed to get field.", e);
            }

            SyncClient tsClient = mock(SyncClient.class);
            try {
                Field field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
                field.setAccessible(true);
                field.set(docStore, tsClient);

            } catch (Exception e) {
                Assertions.fail("Failed to get field.");
            }

            Map<String, ColumnValue> av = AliCodec.encodeDoc(new Document(book));

            RowDeleteChange delete = new RowDeleteChange("table", PrimaryKeyBuilder.createPrimaryKeyBuilder()
                    .addPrimaryKeyColumn("title", PrimaryKeyValue.fromColumn(av.get("title")))
                    .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromColumn(av.get("publisher"))).build());

            DeleteRowResponse mockResponse = mock(DeleteRowResponse.class);
            when(tsClient.deleteRow(any())).thenReturn(mockResponse);

            when(mockResponse.getRequestId()).thenReturn("requestId1");
            Assertions.assertDoesNotThrow(() -> docStore.runDelete(delete, null, null));
        }
    }

    @Test
    void testNewDelete() {
        TestAction delete = new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);

        Assertions.assertDoesNotThrow(() -> docStore.newDelete(delete, null));
    }

    @Test
    void testRunGetQueryWithGlobalIndex() {
        // the equality check is on field which is primary key of global index and non-key attribute
        // of the base table and the local indexes, therefor the global index should be used here.
        Query query = new Query(ali).where("author", FilterOperation.EQUAL, "value");
        query.setFieldPaths(List.of("price"));

        try {
            Field field = ali.getClass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(ali, syncClient);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        DocumentIterator iterator = ali.runGetQuery(query);
        Map<String, String> testMap = new HashMap<>();
        iterator.next(new Document(testMap));
        Assertions.assertEquals(2, testMap.size());

        ArgumentCaptor<SQLQueryRequest> captor = ArgumentCaptor.forClass(SQLQueryRequest.class);
        verify(syncClient, times(1)).sqlQuery(captor.capture());
        SQLQueryRequest sqlQueryRequest = captor.getValue();
        Assertions.assertEquals("SELECT price FROM global_index_3 WHERE author = 'value';",
                sqlQueryRequest.getQuery(), "Global index is not used as expected");
    }

    @Test
    void testRunGetQueryWithLocalIndex() {
        // there is equality check on the partition key but the order by is on price
        // which is the short key of the local index 1 which should be used.
        Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value")
                .where("price", FilterOperation.EQUAL, 3.99).orderBy("price", true);
        query.setFieldPaths(List.of("price"));

        try {
            Field field = ali.getClass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(ali, syncClient);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        DocumentIterator iterator = ali.runGetQuery(query);
        Map<String, String> testMap = new HashMap<>();
        iterator.next(new Document(testMap));
        Assertions.assertEquals(2, testMap.size());

        ArgumentCaptor<SQLQueryRequest> captor = ArgumentCaptor.forClass(SQLQueryRequest.class);
        verify(syncClient, times(1)).sqlQuery(captor.capture());
        SQLQueryRequest sqlQueryRequest = captor.getValue();
        Assertions.assertEquals("SELECT price FROM local_index_1 WHERE title = 'value' AND price = '3.99' ORDER BY price;",
                sqlQueryRequest.getQuery(), "Local index is not used as expected");
    }

    @Test
    void testRunGetQueryWithBaseTable() {
        // Partition key is used for equality check and order by is on sort key of the base table.
        // The base table should be used here.
        Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value")
                .where("publisher", FilterOperation.EQUAL, "John").orderBy("publisher", true);
        query.setFieldPaths(List.of("price"));

        try {
            Field field = ali.getClass().getDeclaredField("tableStoreClient");
            field.setAccessible(true);
            field.set(ali, syncClient);
        } catch (Exception e) {
            Assertions.fail("Failed to get field.");
        }

        DocumentIterator iterator = ali.runGetQuery(query);
        Map<String, String> testMap = new HashMap<>();
        iterator.next(new Document(testMap));
        Assertions.assertEquals(2, testMap.size());

        ArgumentCaptor<SQLQueryRequest> captor = ArgumentCaptor.forClass(SQLQueryRequest.class);
        verify(syncClient, times(1)).sqlQuery(captor.capture());
        SQLQueryRequest sqlQueryRequest = captor.getValue();
        Assertions.assertEquals("SELECT price FROM my-table WHERE title = 'value' AND publisher = 'John' ORDER BY publisher;",
                sqlQueryRequest.getQuery(), "Base index is not used as expected");
    }
}
