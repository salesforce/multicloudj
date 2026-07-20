package com.salesforce.multicloudj.docstore.ali;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CommitTransactionRequest;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
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
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AliDocStoreTest {
  BookWithoutNest book =
      new BookWithoutNest(
          "YellowBook", "Neil", "WA", Timestamp.newBuilder().setNanos(1000).build(), 3.99f, null);
  TestDocStore docStore;
  SyncClient syncClient;
  IndexMeta localIndex1;
  IndexMeta localIndex2;
  IndexMeta globalIndex1;

  static class TestDocStore extends AliDocStore {
    @Override
    public void close() {}
  }

  static class TestAction extends Action {
    public TestAction(
        ActionKind kind, Document document, List<String> fieldPaths, Map<String, Object> mods) {
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
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .withMaxOutstandingActionRPCs(10)
            .build();
    try {
      Field field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
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
    StartLocalTransactionResponse txResponse = mock(StartLocalTransactionResponse.class);
    when(txResponse.getTransactionID()).thenReturn("tx-id");
    when(syncClient.startLocalTransaction(any())).thenReturn(txResponse);
    when(syncClient.commitTransaction(new CommitTransactionRequest("tx-id"))).thenReturn(null);
    StsCredentials stsCredentials = new StsCredentials("key-1", "secret-1", "token-1");
    CredentialsOverrider credsOverrider =
        new CredentialsOverrider.Builder(CredentialsType.SESSION)
            .withSessionCredentials(stsCredentials)
            .build();

    ali =
        new AliDocStore.Builder()
            .withRegion("cn-shanghai")
            .withEndpointType("internet")
            .withInstanceId("something")
            .withCollectionOptions(
                new CollectionOptions.CollectionOptionsBuilder()
                    .withPartitionKey("title")
                    .withSortKey("publisher")
                    .withTableName("my-table")
                    .withRevisionField("docRevision")
                    .build())
            .withCredentialsOverrider(credsOverrider)
            .withTableStoreClient(syncClient)
            .build();

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

    when(syncClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(mockDescribeTableResponse);
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
  void testMapException() {
    AliDocStore docStore = new AliDocStore();
    Assertions.assertInstanceOf(
        InvalidArgumentException.class, docStore.mapException(new ClientException("hello", "")));

    Assertions.assertInstanceOf(
        UnAuthorizedException.class,
        docStore.mapException(new TableStoreException("does-not-matter", "OTSAuthFailed")));
  }

  @Test
  void testCreateDocStoreWithEndpoint() {
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("chameleon-ali-test")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .build();

    ali =
        new AliDocStore.Builder()
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
    TestAction replace =
        new TestAction(ActionKind.ACTION_KIND_REPLACE, new Document(book), null, null);
    Assertions.assertEquals(
        ActionKind.ACTION_KIND_REPLACE,
        docStore.newWriteOperation(replace, null).getAction().getKind());

    TestAction update =
        new TestAction(ActionKind.ACTION_KIND_UPDATE, new Document(book), null, null);
    Assertions.assertNull(docStore.newWriteOperation(update, null));

    TestAction delete =
        new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);
    Assertions.assertEquals(
        ActionKind.ACTION_KIND_DELETE,
        docStore.newWriteOperation(delete, null).getAction().getKind());
  }

  @Test
  void testNewPutWithCreate() {
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);
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
    ActionList writes =
        ali.getActions()
            .create(new Document(bookObj))
            .create(new Document(bookOb2))
            .enableAtomicWrites()
            .create(new Document(bookObjTx1))
            .create(new Document(bookObjTx2))
            .create(new Document(bookObjTx3));
    writes.run();

    ArgumentCaptor<PutRowRequest> argumentCaptorPutRow =
        ArgumentCaptor.forClass(PutRowRequest.class);
    verify(syncClient, times(5)).putRow(argumentCaptorPutRow.capture());

    int count = 0;
    for (PutRowRequest request : argumentCaptorPutRow.getAllValues()) {
      if (request
          .getRowChange()
          .getPrimaryKey()
          .getPrimaryKeyColumn("title")
          .getValue()
          .asString()
          .equals("BlueBook")) {
        Assertions.assertEquals("tx-id", request.getTransactionId());
        count++;
      }
    }

    Assertions.assertEquals(3, count, "Number of puts in transaction doesn't match");
    ArgumentCaptor<CommitTransactionRequest> argumentCaptorTx =
        ArgumentCaptor.forClass(CommitTransactionRequest.class);
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
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .withMaxOutstandingActionRPCs(10)
            .build();

    List<String> fp1 = new ArrayList<>(List.of("title", "publisher"));
    List<String> fp2 =
        new ArrayList<>(
            List.of("publisher", "title")); // Different order of fp1, treated as a different fp.
    List<String> fp3 = new ArrayList<>(List.of("title"));
    List<String> fp4 = new ArrayList<>(List.of("publisher"));

    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
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

      field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
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
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withMaxOutstandingActionRPCs(10)
            .build();

    List<String> fp1 = new ArrayList<>(List.of("title", "publisher"));
    List<String> fp2 =
        new ArrayList<>(
            List.of("publisher", "title")); // Different order of fp1, treated as a different fp.

    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
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

      field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
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

    BatchGetRowResponse.RowResult item =
        new BatchGetRowResponse.RowResult(
            "table",
            new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder().build(),
                List.of(title, publisher, author)),
            null,
            0);

    responses.add(0, item);

    when(mockResponse.getBatchGetRowResult("table")).thenReturn(responses);
    Assertions.assertDoesNotThrow(() -> docStore.runGets(gets, null, 1));
  }

  @Test
  void testBatchGetWithMissingKeys() {
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
    List<Action> gets = new ArrayList<>();
    List<String> fp1 = new ArrayList<>(List.of("d"));
    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
    gets.add(get1);

    TestDocStore docStore = new TestDocStore();
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withMaxOutstandingActionRPCs(10)
            .build();
    try {
      Field field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
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
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
    List<Action> gets = new ArrayList<>();
    List<String> fp1 = new ArrayList<>(List.of("title", "publisher", "author"));
    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(bookObj), fp1, null);
    get1.setKey("partitionKey:YellowBook,sortKey:WA");
    gets.add(get1);

    TestDocStore docStore = new TestDocStore();
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withMaxOutstandingActionRPCs(10)
            .build();

    SyncClient tsClient = mock(SyncClient.class);
    try {
      Field field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
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

    BatchGetRowResponse.RowResult item =
        new BatchGetRowResponse.RowResult(
            "table",
            new Row(
                PrimaryKeyBuilder.createPrimaryKeyBuilder().build(),
                List.of(title, publisher, author)),
            null,
            0);

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
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook",
            null,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            0,
            "something");
    Document document = new Document(bookObj);
    Assertions.assertDoesNotThrow(() -> ali.getActions().put(document).run());
  }

  @Test
  void testDeleteWithRevision() {
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook",
            null,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            0,
            "something");
    Document document = new Document(bookObj);
    Assertions.assertDoesNotThrow(() -> ali.getActions().delete(document).run());
  }

  @Test
  void testExceptionHandling() {
    TableStoreException tsException = new TableStoreException("test1", "OTSNoPermissionAccess");
    Assertions.assertInstanceOf(UnAuthorizedException.class, ali.mapException(tsException));

    ClientException clientException = new ClientException();
    Assertions.assertInstanceOf(InvalidArgumentException.class, ali.mapException(clientException));
  }

  @Test
  void testRunPut() {
    TestDocStore docStore = new TestDocStore();
    SyncClient tsClient = mock(SyncClient.class);
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("table")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .withMaxOutstandingActionRPCs(10)
            .build();
    try {
      Field field =
          docStore.getClass().getSuperclass().getSuperclass().getDeclaredField("collectionOptions");
      field.setAccessible(true);
      field.set(docStore, collectionOptions);

      field = docStore.getClass().getSuperclass().getDeclaredField("tableStoreClient");
      field.setAccessible(true);
      field.set(docStore, tsClient);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    Map<String, ColumnValue> av = AliCodec.encodeDoc(new Document(book));
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);

    RowPutChange put =
        new RowPutChange(
            "table",
            PrimaryKeyBuilder.createPrimaryKeyBuilder()
                .addPrimaryKeyColumn("title", PrimaryKeyValue.fromColumn(av.get("title")))
                .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromColumn(av.get("publisher")))
                .build());

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
      CollectionOptions collectionOptions =
          new CollectionOptions.CollectionOptionsBuilder()
              .withTableName("table")
              .withPartitionKey("title")
              .withSortKey("publisher")
              .withAllowScans(false)
              .withMaxOutstandingActionRPCs(10)
              .build();
      BookWithoutNest bookObj =
          new BookWithoutNest(
              "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
      List<Action> deletes = new ArrayList<>();

      TestAction delete1 =
          new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(bookObj), null, null);
      delete1.setKey("partitionKey:YellowBook,sortKey:WA");

      deletes.add(delete1);

      try {
        Field field = docStore.getClass().getSuperclass().getDeclaredField("batchSize");
        field.setAccessible(true);
        field.set(docStore, 1);

        field =
            docStore
                .getClass()
                .getSuperclass()
                .getSuperclass()
                .getDeclaredField("collectionOptions");
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

      RowDeleteChange delete =
          new RowDeleteChange(
              "table",
              PrimaryKeyBuilder.createPrimaryKeyBuilder()
                  .addPrimaryKeyColumn("title", PrimaryKeyValue.fromColumn(av.get("title")))
                  .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromColumn(av.get("publisher")))
                  .build());

      DeleteRowResponse mockResponse = mock(DeleteRowResponse.class);
      when(tsClient.deleteRow(any())).thenReturn(mockResponse);

      when(mockResponse.getRequestId()).thenReturn("requestId1");
      Assertions.assertDoesNotThrow(() -> docStore.runDelete(delete, null, null));
    }
  }

  @Test
  void testNewDelete() {
    TestAction delete =
        new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);

    Assertions.assertDoesNotThrow(() -> docStore.newDelete(delete, null));
  }

  private void wireMockClient() {
    try {
      Field field = ali.getClass().getDeclaredField("tableStoreClient");
      field.setAccessible(true);
      field.set(ali, syncClient);
    } catch (Exception e) {
      Assertions.fail("Failed to set tableStoreClient field.", e);
    }
  }

  // Captures the RangeRowQueryCriteria issued by running the query through the GetRange path.
  private RangeRowQueryCriteria capturedRangeCriteria(Query query) {
    wireMockClient();
    GetRangeResponse resp =
        new GetRangeResponse(new Response(), new ConsumedCapacity(new CapacityUnit()));
    resp.setRows(new ArrayList<>());
    resp.setNextStartPrimaryKey(null);
    when(syncClient.getRange(any(GetRangeRequest.class))).thenReturn(resp);

    DocumentIterator iterator = ali.runGetQuery(query);
    iterator.hasNext(); // GetRange fetch is lazy; drive one page

    ArgumentCaptor<GetRangeRequest> captor = ArgumentCaptor.forClass(GetRangeRequest.class);
    verify(syncClient, times(1)).getRange(captor.capture());
    return captor.getValue().getRangeRowQueryCriteria();
  }

  @Test
  void testRunGetQueryWithGlobalIndex() {
    // the equality check is on field which is primary key of global index and non-key attribute
    // of the base table and the local indexes, therefore the global index should be used here.
    Query query = new Query(ali).where("author", FilterOperation.EQUAL, "value");
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    Assertions.assertEquals(
        "Index: global_index_3", ali.queryPlan(query), "Global index is not used as expected");
    // And the query executes against that index via GetRange.
    Assertions.assertEquals("global_index_3", capturedRangeCriteria(query).getTableName());
  }

  @Test
  void testRunGetQueryWithLocalIndex() {
    // there is equality check on the partition key but the order by is on price
    // which is the short key of the local index 1 which should be used.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("price", FilterOperation.EQUAL, 3.99)
            .orderBy("price", true);
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    Assertions.assertEquals(
        "Index: local_index_1", ali.queryPlan(query), "Local index is not used as expected");
    Assertions.assertEquals("local_index_1", capturedRangeCriteria(query).getTableName());
  }

  @Test
  void testRunGetQueryWithBaseTable() {
    // Partition key is used for equality check and order by is on sort key of the base table.
    // The base table should be used here.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.EQUAL, "John")
            .orderBy("publisher", true);
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    Assertions.assertEquals(
        "Table: my-table", ali.queryPlan(query), "Base table is not used as expected");
    Assertions.assertEquals("my-table", capturedRangeCriteria(query).getTableName());
  }

  @Test
  void testProjectedQueryForcesPrimaryKeyColumns() {
    // A projected query that omits the key columns must still fetch them: Tablestore's GetRange
    // drops a row whose requested columns are all absent, and both decode and the pagination cursor
    // read the primary key. So columns_to_get must include the projected field plus both PK cols.
    Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value");
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    Set<String> columns = capturedRangeCriteria(query).getColumnsToGet();
    Assertions.assertTrue(columns.contains("price"), "projected field should be requested");
    Assertions.assertTrue(columns.contains("title"), "partition key should be force-added");
    Assertions.assertTrue(columns.contains("publisher"), "sort key should be force-added");
    Assertions.assertEquals(3, columns.size(), "only the field + both PK columns, no extras");
  }

  @Test
  void testProjectedQueryIncludingKeyDoesNotDuplicate() {
    // When the projection already names a key column, it must not be added twice.
    Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value");
    query.setFieldPaths(List.of("price", "title"));

    wireMockClient();
    Set<String> columns = capturedRangeCriteria(query).getColumnsToGet();
    Assertions.assertTrue(columns.contains("price"));
    Assertions.assertTrue(columns.contains("title"));
    Assertions.assertTrue(columns.contains("publisher"), "missing sort key still force-added");
    Assertions.assertEquals(3, columns.size(), "already-projected key not duplicated");
  }

  @Test
  void testUnprojectedQueryRequestsAllColumns() {
    // No field paths means "all columns": columns_to_get must stay empty so Tablestore returns the
    // full row (adding a subset here would wrongly restrict the projection).
    Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value");

    wireMockClient();
    Assertions.assertTrue(
        capturedRangeCriteria(query).getColumnsToGet().isEmpty(),
        "an unprojected query must not restrict columns_to_get");
  }

  // Builds a store over "my-table" (PK title+publisher) with the given AllowScans setting, wired to
  // the mock client and its describeTable stub.
  private AliDocStore storeWithAllowScans(boolean allowScans) {
    AliDocStore store =
        new AliDocStore.Builder()
            .withRegion("cn-shanghai")
            .withEndpointType("internet")
            .withInstanceId("something")
            .withCollectionOptions(
                new CollectionOptions.CollectionOptionsBuilder()
                    .withPartitionKey("title")
                    .withSortKey("publisher")
                    .withTableName("my-table")
                    .withRevisionField("docRevision")
                    .withAllowScans(allowScans)
                    .build())
            .withCredentialsOverrider(
                new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(new StsCredentials("k", "s", "t"))
                    .build())
            .withTableStoreClient(syncClient)
            .build();
    try {
      Field field = store.getClass().getDeclaredField("tableStoreClient");
      field.setAccessible(true);
      field.set(store, syncClient);
    } catch (Exception e) {
      Assertions.fail("Failed to set tableStoreClient field.", e);
    }
    return store;
  }

  @Test
  void testScanWithOrderByIsRejected() {
    // No equality on the partition key -> no queryable -> scan; an order-by on a scan is rejected.
    AliDocStore store = storeWithAllowScans(true);
    Query query = new Query(store).where("publisher", FilterOperation.EQUAL, "John").orderBy(
        "publisher", true);
    Assertions.assertThrows(InvalidArgumentException.class, () -> store.runGetQuery(query));
  }

  @Test
  void testScanRejectedWhenAllowScansFalse() {
    AliDocStore store = storeWithAllowScans(false);
    // Filter on a non-partition-key field, no order-by -> scan; disallowed.
    Query query = new Query(store).where("publisher", FilterOperation.EQUAL, "John");
    Assertions.assertThrows(InvalidArgumentException.class, () -> store.runGetQuery(query));
  }

  @Test
  void testScanAllowedWhenAllowScansTrue() {
    AliDocStore store = storeWithAllowScans(true);
    GetRangeResponse resp =
        new GetRangeResponse(new Response(), new ConsumedCapacity(new CapacityUnit()));
    resp.setRows(new ArrayList<>());
    resp.setNextStartPrimaryKey(null);
    when(syncClient.getRange(any(GetRangeRequest.class))).thenReturn(resp);

    Query query = new Query(store).where("publisher", FilterOperation.EQUAL, "John");
    DocumentIterator iter = store.runGetQuery(query);
    Assertions.assertInstanceOf(AliDocumentIterator.class, iter);
    iter.hasNext();
    ArgumentCaptor<GetRangeRequest> captor = ArgumentCaptor.forClass(GetRangeRequest.class);
    verify(syncClient, times(1)).getRange(captor.capture());
    // A scan ranges over the whole base table.
    RangeRowQueryCriteria criteria = captor.getValue().getRangeRowQueryCriteria();
    Assertions.assertEquals("my-table", criteria.getTableName());
  }

  @Test
  void testInQueryRoutesThroughGetRangeAndPaginates() {
    // IN on the partition key: no key equality -> scan over base table, IN enforced by column
    // filter; crucially this now goes through GetRange (so it can paginate), not SQL.
    AliDocStore store = storeWithAllowScans(true);
    GetRangeResponse resp =
        new GetRangeResponse(new Response(), new ConsumedCapacity(new CapacityUnit()));
    resp.setRows(new ArrayList<>());
    resp.setNextStartPrimaryKey(null);
    when(syncClient.getRange(any(GetRangeRequest.class))).thenReturn(resp);

    Query query = new Query(store).where("title", FilterOperation.IN, List.of("a", "b"));
    DocumentIterator iter = store.runGetQuery(query);
    Assertions.assertInstanceOf(AliDocumentIterator.class, iter);
    iter.hasNext();
    ArgumentCaptor<GetRangeRequest> captor = ArgumentCaptor.forClass(GetRangeRequest.class);
    verify(syncClient, times(1)).getRange(captor.capture());
    // IN cannot narrow the key range, so a column filter must be present to enforce membership.
    Assertions.assertNotNull(captor.getValue().getRangeRowQueryCriteria().getFilter());
  }

  // covering-index selection for all-fields queries

  // Builds a store over base table PK [game, player] + defined columns [score, time, glitch], wired
  // to a describeTable stub carrying the given global indexes. Mirrors how the conformance table is
  // provisioned (all attribute columns pre-defined so indexes can reference them).
  private AliDocStore storeWithSchema(IndexMeta... indexes) {
    AliDocStore store =
        new AliDocStore.Builder()
            .withRegion("cn-shanghai")
            .withEndpointType("internet")
            .withInstanceId("something")
            .withCollectionOptions(
                new CollectionOptions.CollectionOptionsBuilder()
                    .withPartitionKey("game")
                    .withSortKey("player")
                    .withTableName("scores")
                    .withRevisionField("docRevision")
                    .withAllowScans(false)
                    .build())
            .withCredentialsOverrider(
                new CredentialsOverrider.Builder(CredentialsType.SESSION)
                    .withSessionCredentials(new StsCredentials("k", "s", "t"))
                    .build())
            .withTableStoreClient(syncClient)
            .build();

    TableMeta meta = new TableMeta("scores");
    meta.addPrimaryKeyColumn(
        "game", com.alicloud.openservices.tablestore.model.PrimaryKeyType.STRING);
    meta.addPrimaryKeyColumn(
        "player", com.alicloud.openservices.tablestore.model.PrimaryKeyType.STRING);
    meta.addDefinedColumn(
        "score", com.alicloud.openservices.tablestore.model.DefinedColumnType.INTEGER);
    meta.addDefinedColumn(
        "time", com.alicloud.openservices.tablestore.model.DefinedColumnType.STRING);
    meta.addDefinedColumn(
        "glitch", com.alicloud.openservices.tablestore.model.DefinedColumnType.BOOLEAN);

    DescribeTableResponse desc = new DescribeTableResponse(new Response());
    desc.setTableMeta(meta);
    for (IndexMeta idx : indexes) {
      desc.addIndexMeta(idx);
    }
    when(syncClient.describeTable(any(DescribeTableRequest.class))).thenReturn(desc);
    return store;
  }

  private IndexMeta globalIndex(String name, List<String> pk, List<String> defined) {
    IndexMeta idx = new IndexMeta(name);
    idx.setIndexType(IndexType.IT_GLOBAL_INDEX);
    for (String c : pk) {
      idx.addPrimaryKeyColumn(c);
    }
    for (String c : defined) {
      idx.addDefinedColumn(c);
    }
    return idx;
  }

  @Test
  void testAllFieldsQueryUsesCoveringGlobalIndex() {
    // gsi is COVERING: its PK [player, time, game] + defined [score, glitch] == every base column
    // {game, player, score, time, glitch}. An all-fields query (no setFieldPaths) must select it.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Assertions.assertEquals(
        "Index: gsi", store.queryPlan(query), "Covering global index should serve all-fields");
  }

  @Test
  void testAllFieldsQueryRejectsNonCoveringGlobalIndex() {
    // gsiPartial omits base column 'glitch' -> NOT covering. An all-fields query cannot be served
    // from it, so no queryable is resolved; with order-by present this is rejected (would-be scan).
    IndexMeta gsiPartial =
        globalIndex("gsiPartial", List.of("player", "time", "game"), List.of("score"));
    AliDocStore store = storeWithSchema(gsiPartial);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Assertions.assertThrows(InvalidArgumentException.class, () -> store.runGetQuery(query));
  }

  @Test
  void testProjectedQueryUsesNonCoveringIndexWhenItHasTheFields() {
    // Even a non-covering index serves a query whose EXPLICIT projection it happens to contain.
    IndexMeta gsiPartial =
        globalIndex("gsiPartial", List.of("player", "time", "game"), List.of("score"));
    AliDocStore store = storeWithSchema(gsiPartial);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of("score")); // score is in the index

    Assertions.assertEquals("Index: gsiPartial", store.queryPlan(query));
  }
}
