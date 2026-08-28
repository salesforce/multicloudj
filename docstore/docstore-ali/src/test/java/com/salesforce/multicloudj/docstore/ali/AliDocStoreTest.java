package com.salesforce.multicloudj.docstore.ali;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.model.AbortTransactionRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.CapacityUnit;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CommitTransactionRequest;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.ConsumedCapacity;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.DeleteRowResponse;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.PutRowResponse;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Response;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.TableMeta;
import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.FailedPreconditionException;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.exceptions.UnAuthorizedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
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
import java.util.function.Consumer;
import java.util.function.Predicate;
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

  // A RowChange subtype that is neither a put nor a delete, used to exercise the transactional
  // dispatch loop's rejection of unsupported change types. Not tied to any real SDK operation, so
  // it stays valid regardless of which RowChange types the driver supports.
  static class UnknownRowChange extends RowChange {
    UnknownRowChange(String tableName) {
      super(tableName);
    }

    @Override
    public int getDataSize() {
      return 0;
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
  void testRunPutRethrowsNonConditionalFailure() {
    // A non-conditional Tablestore failure (e.g. server busy) means the row was NOT written.
    // runPut must re-throw it rather than swallow it, so a lost write is not treated as success.
    when(syncClient.putRow(any()))
        .thenThrow(new TableStoreException("server busy", "OTSServerBusy"));
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);
    RowPutChange rowChange = new RowPutChange("my-table");

    TableStoreException thrown =
        Assertions.assertThrows(
            TableStoreException.class, () -> ali.runPut(rowChange, create, null));
    Assertions.assertEquals("OTSServerBusy", thrown.getErrorCode());
  }

  @Test
  void testRunPutMapsConditionalFailureOnCreate() {
    // A conditional-check failure on CREATE means the row already exists.
    when(syncClient.putRow(any()))
        .thenThrow(new TableStoreException("exists", "OTSConditionCheckFail"));
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);
    RowPutChange rowChange = new RowPutChange("my-table");

    Assertions.assertThrows(
        ResourceAlreadyExistsException.class, () -> ali.runPut(rowChange, create, null));
  }

  @Test
  void testRunPutMapsConditionalFailureOnReplace() {
    // A conditional-check failure on a non-CREATE (e.g. REPLACE) means the row does not exist.
    when(syncClient.putRow(any()))
        .thenThrow(new TableStoreException("missing", "OTSConditionCheckFail"));
    TestAction replace =
        new TestAction(ActionKind.ACTION_KIND_REPLACE, new Document(book), null, null);
    RowPutChange rowChange = new RowPutChange("my-table");

    Assertions.assertThrows(
        ResourceNotFoundException.class, () -> ali.runPut(rowChange, replace, null));
  }

  @Test
  void testRunPutRethrowsNullErrorCodeFailure() {
    // A TableStoreException with a null error code is not a conditional-check failure, so it must
    // still be re-thrown (not swallowed) rather than treated as a successful write.
    when(syncClient.putRow(any())).thenThrow(new TableStoreException("boom", null));
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);
    RowPutChange rowChange = new RowPutChange("my-table");

    TableStoreException thrown =
        Assertions.assertThrows(
            TableStoreException.class, () -> ali.runPut(rowChange, create, null));
    Assertions.assertNull(thrown.getErrorCode());
  }

  @Test
  void testFailedWriteDoesNotStampRevision() {
    // When the write fails, the document's revision field must NOT be stamped with a new revision
    // -- otherwise the client would carry a revision the server never persisted. The revision is
    // stamped by updateRevision, which runs only after the write futures join successfully.
    when(syncClient.putRow(any()))
        .thenThrow(new TableStoreException("server busy", "OTSServerBusy"));
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
    Document document = new Document(bookObj);
    Assertions.assertNull(
        document.getField("docRevision"), "precondition: no revision before the write");

    // The failure must surface through the full run() path with the classified type: run() routes
    // the re-thrown TableStoreException through mapException, and OTSServerBusy maps to
    // UnknownException. Asserting the concrete type guards the classification, not just that
    // something was thrown.
    Assertions.assertThrows(
        UnknownException.class, () -> ali.getActions().put(document).run());

    Assertions.assertNull(
        document.getField("docRevision"),
        "a failed write must not stamp a revision the server never persisted");
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
  void testWritesTxMixedPutAndDelete() {
    // An atomic write containing both a put and a delete must issue the delete via deleteRow (not
    // putRow) so the row is actually removed. The transactional path used to send every operation
    // through putRow, so a delete never reached deleteRow inside the transaction. Verify the delete
    // lands on deleteRow and both operations carry the transaction id.
    Book putBook = new Book("BlueBook", null, "WA", null, 3.99f, null, null);
    Book deleteBook = new Book("BlueBook", null, "TX", null, 3.99f, null, null);
    ActionList writes =
        ali.getActions()
            .enableAtomicWrites()
            .put(new Document(putBook))
            .delete(new Document(deleteBook));
    writes.run();

    // The delete must go through deleteRow (exactly once), with the transaction id attached.
    ArgumentCaptor<DeleteRowRequest> deleteCaptor =
        ArgumentCaptor.forClass(DeleteRowRequest.class);
    verify(syncClient, times(1)).deleteRow(deleteCaptor.capture());
    DeleteRowRequest deleteRequest = deleteCaptor.getValue();
    Assertions.assertEquals("tx-id", deleteRequest.getTransactionId());
    Assertions.assertEquals(
        "TX",
        deleteRequest
            .getRowChange()
            .getPrimaryKey()
            .getPrimaryKeyColumn("publisher")
            .getValue()
            .asString(),
        "deleteRow must target the row identified by the delete action's key");

    // The put must go through putRow (exactly once), also within the transaction.
    ArgumentCaptor<PutRowRequest> putCaptor = ArgumentCaptor.forClass(PutRowRequest.class);
    verify(syncClient, times(1)).putRow(putCaptor.capture());
    Assertions.assertEquals("tx-id", putCaptor.getValue().getTransactionId());

    // The transaction commits once, covering both operations atomically.
    verify(syncClient, times(1)).commitTransaction(any());
  }

  @Test
  void testWritesTxRejectsUnsupportedRowChange() {
    // The transactional dispatch loop must reject an unsupported RowChange subtype with a clear
    // error instead of blindly casting it to a put and throwing an opaque ClassCastException. Feed
    // it a synthetic unsupported RowChange (via an overridden newPut) and assert the atomic write
    // fails cleanly and rolls back. A test-only stub keeps this valid even if new RowChange types
    // become supported later.
    SyncClient txClient = mock(SyncClient.class);
    StartLocalTransactionResponse txResponse = mock(StartLocalTransactionResponse.class);
    when(txResponse.getTransactionID()).thenReturn("tx-id");
    when(txClient.startLocalTransaction(any())).thenReturn(txResponse);

    AliDocStore.Builder builder =
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
            .withTableStoreClient(txClient);
    AliDocStore docStoreWithUnknownChange =
        new AliDocStore(builder) {
          @Override
          protected WriteOperation newPut(Action action, Consumer<Predicate<Object>> beforeDo) {
            return new WriteOperation(
                action, new UnknownRowChange("my-table"), null, null, () -> {});
          }
        };

    Book book = new Book("BlueBook", null, "WA", null, 3.99f, null, null);
    ActionList writes =
        docStoreWithUnknownChange
            .getActions()
            .enableAtomicWrites()
            .put(new Document(book));

    TransactionFailedException thrown =
        Assertions.assertThrows(TransactionFailedException.class, writes::run);
    Assertions.assertInstanceOf(
        UnSupportedOperationException.class,
        thrown.getCause(),
        "unsupported RowChange must surface as UnSupportedOperationException, not a CCE");

    // The transaction must NOT commit, and must be aborted (all-or-nothing rollback).
    verify(txClient, times(0)).commitTransaction(any());
    verify(txClient, times(1)).abortTransaction(any(AbortTransactionRequest.class));
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
    // A delete whose document carries a revision must attach a revision precondition to the
    // DeleteRowRequest so the delete is guarded by optimistic concurrency.
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

    ArgumentCaptor<DeleteRowRequest> captor = ArgumentCaptor.forClass(DeleteRowRequest.class);
    verify(syncClient).deleteRow(captor.capture());
    Condition condition = captor.getValue().getRowChange().getCondition();
    Assertions.assertNotNull(
        condition.getColumnCondition(),
        "delete with a revision must carry a revision column precondition");
  }

  @Test
  void testDeleteWithoutRevisionIsUnconditional() {
    // A delete whose document has no revision must issue an unconditional delete (no column
    // precondition) -- deletes are idempotent when the caller does not pin a revision.
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, null);
    Document document = new Document(bookObj);
    Assertions.assertDoesNotThrow(() -> ali.getActions().delete(document).run());

    ArgumentCaptor<DeleteRowRequest> captor = ArgumentCaptor.forClass(DeleteRowRequest.class);
    verify(syncClient).deleteRow(captor.capture());
    Condition condition = captor.getValue().getRowChange().getCondition();
    Assertions.assertNull(
        condition.getColumnCondition(),
        "delete without a revision must not carry a column precondition");
  }

  @Test
  void testDeleteWithStaleRevisionMapsToFailedPrecondition() {
    // A delete never sets an existence expectation, so its only conditional-check failure is a
    // revision-precondition mismatch (optimistic-lock loss). That must surface as
    // FailedPreconditionException (via the default OTSConditionCheckFail mapping), NOT
    // ResourceNotFoundException -- the row is present, just at a different revision.
    when(syncClient.deleteRow(any()))
        .thenThrow(new TableStoreException("stale", "OTSConditionCheckFail"));
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook",
            null,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            0,
            "stale-revision");
    Document document = new Document(bookObj);
    Assertions.assertThrows(
        FailedPreconditionException.class, () -> ali.getActions().delete(document).run());
  }

  @Test
  void testDeleteRethrowsNonConditionalFailure() {
    // A non-conditional TableStoreException on a delete must be re-thrown (not swallowed and not
    // mis-mapped to ResourceNotFoundException) so it reaches mapException and is classified by its
    // error code -- OTSServerBusy maps to UnknownException.
    when(syncClient.deleteRow(any()))
        .thenThrow(new TableStoreException("boom", "OTSServerBusy"));
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook",
            null,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            0,
            "something");
    Document document = new Document(bookObj);
    Assertions.assertThrows(
        UnknownException.class, () -> ali.getActions().delete(document).run());
  }

  @Test
  void testAtomicDeleteWithStaleRevisionFailsAndAborts() {
    // A stale-revision delete inside an atomic write makes the transactional deleteRow fail the
    // conditional check; the whole transaction must fail (TransactionFailedException) and abort,
    // preserving all-or-nothing semantics.
    when(syncClient.deleteRow(any()))
        .thenThrow(new TableStoreException("stale", "OTSConditionCheckFail"));
    Book putBook = new Book("BlueBook", null, "WA", null, 3.99f, null, null);
    BookWithoutNest deleteBook =
        new BookWithoutNest(
            "BlueBook", null, "TX", Timestamp.newBuilder().setNanos(1000).build(), 0,
            "stale-revision");
    ActionList writes =
        ali.getActions()
            .enableAtomicWrites()
            .put(new Document(putBook))
            .delete(new Document(deleteBook));

    Assertions.assertThrows(TransactionFailedException.class, writes::run);
    verify(syncClient, times(0)).commitTransaction(any());
    verify(syncClient, times(1)).abortTransaction(any(AbortTransactionRequest.class));
  }

  @Test
  void testAtomicDeleteCarriesRevisionPrecondition() {
    // Defense-in-depth for the transactional path: a revision-bearing delete inside an atomic write
    // must carry the revision precondition on the transactional DeleteRowRequest (with the txn id),
    // not just on the non-atomic path.
    DeleteRowResponse deleteResponse = mock(DeleteRowResponse.class);
    when(syncClient.deleteRow(any())).thenReturn(deleteResponse);
    BookWithoutNest deleteBook =
        new BookWithoutNest(
            "BlueBook", null, "TX", Timestamp.newBuilder().setNanos(1000).build(), 0, "rev-1");
    ali.getActions().enableAtomicWrites().delete(new Document(deleteBook)).run();

    ArgumentCaptor<DeleteRowRequest> captor = ArgumentCaptor.forClass(DeleteRowRequest.class);
    verify(syncClient).deleteRow(captor.capture());
    DeleteRowRequest request = captor.getValue();
    Assertions.assertEquals("tx-id", request.getTransactionId());
    Assertions.assertNotNull(
        request.getRowChange().getCondition().getColumnCondition(),
        "transactional delete must carry the revision column precondition");
  }

  @Test
  void testDeleteWithEmptyStringRevisionIsUnconditional() {
    // An empty-string revision means "no revision pinned": buildRevisionPrecondition returns null,
    // so the delete is unconditional (no column precondition), same as a null revision.
    BookWithoutNest bookObj =
        new BookWithoutNest(
            "YellowBook", null, "WA", Timestamp.newBuilder().setNanos(1000).build(), 0, "");
    Document document = new Document(bookObj);
    Assertions.assertDoesNotThrow(() -> ali.getActions().delete(document).run());

    ArgumentCaptor<DeleteRowRequest> captor = ArgumentCaptor.forClass(DeleteRowRequest.class);
    verify(syncClient).deleteRow(captor.capture());
    Assertions.assertNull(
        captor.getValue().getRowChange().getCondition().getColumnCondition(),
        "empty-string revision must not attach a column precondition");
  }

  @Test
  void testDeleteWithNonStringRevisionThrowsInvalidArgument() {
    // A non-String revision on a delete is a caller error: buildRevisionPrecondition rejects it,
    // and it must surface as InvalidArgumentException rather than reaching the server.
    Map<String, Object> doc = new HashMap<>();
    doc.put("title", "YellowBook");
    doc.put("publisher", "WA");
    doc.put("docRevision", 123);
    Document document = new Document(doc);
    Assertions.assertThrows(
        InvalidArgumentException.class, () -> ali.getActions().delete(document).run());
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

  @Test
  void testProjectedQueryFetchesNonProjectedPredicateField() {
    // A projected query whose predicate is on a present-but-not-projected field must still FETCH
    // that field. Tablestore applies columns_to_get BEFORE the server-side column filter, so a
    // filter on an un-fetched field reads as missing and (passIfMissing(false)) drops every match.
    // columns_to_get must therefore include the predicate field 'author' on top of the projected
    // 'price' and both PK columns.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("author", FilterOperation.GREATER_THAN, "x");
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    Set<String> columns = capturedRangeCriteria(query).getColumnsToGet();
    Assertions.assertTrue(
        columns.contains("author"), "non-projected predicate field must be fetched");
    Assertions.assertTrue(columns.contains("price"), "projected field should be requested");
    Assertions.assertTrue(columns.contains("title"), "partition key should be force-added");
    Assertions.assertTrue(columns.contains("publisher"), "sort key should be force-added");
    Assertions.assertEquals(
        4, columns.size(), "projection + both PK columns + the predicate field, no extras");
  }

  @Test
  void testTrimColumnsKeepsVisibleAndPreservesPrimaryKey() {
    // trimColumns keeps only the attribute columns named in the visible set and preserves the
    // primary key, so the caller sees only its projection while the pagination cursor stays intact.
    PrimaryKey pk =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("title", PrimaryKeyValue.fromString("YellowBook"))
            .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromString("WA"))
            .build();
    Row row =
        new Row(
            pk,
            List.of(
                new Column("price", ColumnValue.fromDouble(3.99)),
                new Column("author", ColumnValue.fromString("Neil"))));

    Row trimmed = QueryRunner.trimColumns(row, Set.of("price", "title", "publisher"));

    Assertions.assertNotNull(trimmed.getLatestColumn("price"), "visible column must be kept");
    Assertions.assertNull(
        trimmed.getLatestColumn("author"), "non-visible attribute column must be dropped");
    Assertions.assertEquals(
        pk, trimmed.getPrimaryKey(), "primary key must be preserved for the pagination cursor");
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
