package com.salesforce.multicloudj.docstore.ali;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
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
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.Error;
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
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
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
  void testComputePerRequestLimit() {
    // Package-private helper: the per-request GetRange cap = offset + limit, but only when that
    // budget is below Tablestore's 5000-row page cap (a limit at/above the cap tightens nothing).
    // A non-positive limit means "no caller limit" -> 0 (unbounded), and long arithmetic keeps a
    // pathological offset/limit from overflowing int into a negative that setLimit would reject.
    Assertions.assertEquals(1, AliDocStore.computePerRequestLimit(0, 1));
    Assertions.assertEquals(3, AliDocStore.computePerRequestLimit(1, 2));
    Assertions.assertEquals(4999, AliDocStore.computePerRequestLimit(0, 4999));
    Assertions.assertEquals(
        0, AliDocStore.computePerRequestLimit(0, 5000), "at the cap -> unbounded");
    Assertions.assertEquals(
        0, AliDocStore.computePerRequestLimit(10, 5000), "over the cap -> unbounded");
    Assertions.assertEquals(
        0, AliDocStore.computePerRequestLimit(0, 0), "no limit -> unbounded");
    Assertions.assertEquals(
        0, AliDocStore.computePerRequestLimit(100, 0), "no limit -> unbounded for any offset");
    Assertions.assertEquals(
        0,
        AliDocStore.computePerRequestLimit(Integer.MAX_VALUE, Integer.MAX_VALUE),
        "pathological offset+limit must not overflow; falls through to unbounded");
    Assertions.assertEquals(
        1, AliDocStore.computePerRequestLimit(-5, 1), "negative offset is treated as 0");
  }

  @Test
  void testGetRangeBoundsPageToLimit() {
    // A limit(1) query caps the GetRange page to the caller's total budget (offset 0 + limit 1),
    // so a small-limit query does not scan/hydrate a full page just to yield one row.
    Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value").limit(1);

    wireMockClient();
    Assertions.assertEquals(1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeBoundsPageToOffsetPlusLimit() {
    // The per-request cap is offset + limit: a fixed per-page cap (the offset skipped plus the
    // limit returned), applied to every page rather than tracking the iterator's remaining demand.
    Query query =
        new Query(ali).where("title", FilterOperation.EQUAL, "value").offset(1).limit(2);

    wireMockClient();
    Assertions.assertEquals(3, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenNoLimit() {
    // With no caller limit the request is left unbounded (getLimit() stays the SDK default -1) so
    // Tablestore applies its natural 5000-row/4 MB page cap -- unchanged from before this bounding.
    Query query = new Query(ali).where("title", FilterOperation.EQUAL, "value");

    wireMockClient();
    Assertions.assertEquals(-1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeBoundedWhenKeyRangeTight() {
    // A key-only equality + terminal-range query (partition key equality + sort-key range) is
    // key-range-tight: the range captures the whole predicate set, so the column filter trims at
    // most an O(1) boundary. The per-page cap (offset + limit) therefore applies -> getLimit() = 5.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.GREATER_THAN, "M")
            .limit(5);

    wireMockClient();
    Assertions.assertEquals(5, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenTerminalInclusiveUpperOpensScan() {
    // A partition-key equality plus a terminal sort-key '<=' bound. Forward (ascending, the default
    // order), an inclusive upper bound on the LAST PK column has no trailing key slot to represent
    // it exactly, so the key range is widened fully open above the bound and the column filter
    // trims the tail. The plan is therefore NOT key-range-tight: the per-page cap must stay off
    // (getLimit() = -1) despite limit(5), otherwise the iterator would re-scan page after page when
    // the data skews above "M".
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.LESS_THAN_OR_EQUAL_TO, "M")
            .limit(5);

    wireMockClient();
    Assertions.assertEquals(-1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenRepeatedSameSideBound() {
    // Two lower bounds on the sort-key range column (publisher > "M" AND publisher > "A"): only one
    // can be folded into the key range, the other is enforced solely by the column filter and may
    // be selective, so the plan is NOT key-range-tight. The per-page cap must stay off (getLimit()
    // = -1) despite limit(5), otherwise the iterator would re-scan page after page.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.GREATER_THAN, "M")
            .where("publisher", FilterOperation.GREATER_THAN, "A")
            .limit(5);

    wireMockClient();
    Assertions.assertEquals(-1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenRepeatedSameSideBoundReversedOrder() {
    // Same shape as above with the two > predicates in the opposite insertion order (publisher >
    // "A" then publisher > "M"). The gate is order-independent, so this must also stay unbounded
    // (getLimit() = -1) -- the reversed order retains the STRONGER bound but is still not tight.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.GREATER_THAN, "A")
            .where("publisher", FilterOperation.GREATER_THAN, "M")
            .limit(5);

    wireMockClient();
    Assertions.assertEquals(-1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenNonKeyPredicate() {
    // The #18 shape: an equality on the partition key plus a predicate on a NON-key attribute. The
    // non-key predicate is enforced only by the column filter, which drops a non-trivial number of
    // scanned rows, so the plan is NOT key-range-tight. A small per-page cap would force the
    // iterator to re-scan page after page, so the request must be left unbounded (getLimit() = -1)
    // even though the caller set limit(1).
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("coverType", FilterOperation.EQUAL, "hardback")
            .limit(1);

    wireMockClient();
    Assertions.assertEquals(-1, capturedRangeCriteria(query).getLimit());
  }

  @Test
  void testGetRangeUnboundedWhenInOnKey() {
    // IN on the partition key cannot narrow the key range (the query scans the whole table and the
    // membership is enforced by the column filter), so the plan is NOT tight and the per-page cap
    // must stay off (getLimit() = -1) despite limit(1).
    AliDocStore store = storeWithAllowScans(true);
    GetRangeResponse resp =
        new GetRangeResponse(new Response(), new ConsumedCapacity(new CapacityUnit()));
    resp.setRows(new ArrayList<>());
    resp.setNextStartPrimaryKey(null);
    when(syncClient.getRange(any(GetRangeRequest.class))).thenReturn(resp);

    Query query = new Query(store).where("title", FilterOperation.IN, List.of("a", "b")).limit(1);
    DocumentIterator iter = store.runGetQuery(query);
    iter.hasNext();

    ArgumentCaptor<GetRangeRequest> captor = ArgumentCaptor.forClass(GetRangeRequest.class);
    verify(syncClient, times(1)).getRange(captor.capture());
    Assertions.assertEquals(
        -1,
        captor.getValue().getRangeRowQueryCriteria().getLimit(),
        "IN-on-key must stay unbounded");
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
    return storeWithSchema(false, indexes);
  }

  private AliDocStore storeWithSchema(boolean allowScans, IndexMeta... indexes) {
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
                    .withAllowScans(allowScans)
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

  // Builds an index-shaped primary key [player, time, game] (the gsi/gsiPartial key order used by
  // the storeWithSchema fixtures). Tablestore folds the base key (game, player) into the index key.
  private PrimaryKey indexPk(String player, String time, String game) {
    return PrimaryKeyBuilder.createPrimaryKeyBuilder()
        .addPrimaryKeyColumn("player", PrimaryKeyValue.fromString(player))
        .addPrimaryKeyColumn("time", PrimaryKeyValue.fromString(time))
        .addPrimaryKeyColumn("game", PrimaryKeyValue.fromString(game))
        .build();
  }

  // Builds a base-table primary key [game, player] (the "scores" base key order). The order matches
  // baseKeyColumns so it equals the key hydration derives from an index row.
  private PrimaryKey basePk(String game, String player) {
    return PrimaryKeyBuilder.createPrimaryKeyBuilder()
        .addPrimaryKeyColumn("game", PrimaryKeyValue.fromString(game))
        .addPrimaryKeyColumn("player", PrimaryKeyValue.fromString(player))
        .build();
  }

  // A GetRange page carrying the given rows and continuation cursor (null = range exhausted).
  private GetRangeResponse getRangeResponse(List<Row> rows, PrimaryKey next) {
    GetRangeResponse resp =
        new GetRangeResponse(new Response(), new ConsumedCapacity(new CapacityUnit()));
    resp.setRows(rows);
    resp.setNextStartPrimaryKey(next);
    return resp;
  }

  // A BatchGetRow response over the given table, one RowResult per base row. A null row models a
  // base row that was concurrently deleted, so BatchGetRow returns a succeeded result with no row.
  private BatchGetRowResponse batchGetResponse(String table, List<Row> baseRows) {
    BatchGetRowResponse resp = new BatchGetRowResponse(new Response());
    for (int i = 0; i < baseRows.size(); i++) {
      resp.addResult(
          new BatchGetRowResponse.RowResult(
              table, baseRows.get(i), new ConsumedCapacity(new CapacityUnit()), i));
    }
    return resp;
  }

  // A BatchGetRow response whose single sub-row FAILED (isSucceed()==false), modeling a throttled
  // or partial server error that outlived the client's own request-level retries. index must match
  // the sub-row's position in the hydration criteria so createRequestForRetry can re-issue it.
  private BatchGetRowResponse failedBatchGetResponse(String table, String errorCode, int index) {
    BatchGetRowResponse resp = new BatchGetRowResponse(new Response());
    resp.addResult(
        new BatchGetRowResponse.RowResult(
            table, new Error(errorCode, errorCode + " (test)"), index));
    return resp;
  }

  // A BatchGetRow response with one FAILED sub-row per error code, at sequential indices 0..n-1
  // matching the sub-rows' positions in the hydration criteria. Models a partial-failure batch that
  // mixes retryable and non-retryable per-row errors in a single response.
  private BatchGetRowResponse failedBatchGetResponse(String table, List<String> errorCodes) {
    BatchGetRowResponse resp = new BatchGetRowResponse(new Response());
    for (int i = 0; i < errorCodes.size(); i++) {
      resp.addResult(
          new BatchGetRowResponse.RowResult(
              table, new Error(errorCodes.get(i), errorCodes.get(i) + " (test)"), i));
    }
    return resp;
  }

  // Drains an iterator into a list of decoded documents (one map per row).
  private List<Map<String, Object>> drainAll(DocumentIterator iter) {
    List<Map<String, Object>> out = new ArrayList<>();
    while (iter.hasNext()) {
      Map<String, Object> m = new HashMap<>();
      iter.next(new Document(m));
      out.add(m);
    }
    return out;
  }

  @Test
  void testUnprojectedOrderedQueryUsesIndexAndHydrates() {
    // An unprojected ordered query on the index's partition + sort key uses the index for candidate
    // selection and ordering, then hydrates each row from the base table so columns the index does
    // not carry (including schema-less attributes) are recovered. It must NOT silently fall back to
    // base/scan order or reject the ordering.
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
        "Index: gsi",
        store.queryPlan(query),
        "unprojected ordered query must use the index for ordering, not fall back to a scan");

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("time", ColumnValue.fromString("2024-03-01")),
                new Column("glitch", ColumnValue.fromBoolean(false)),
                new Column("bonus", ColumnValue.fromLong(99))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    List<Map<String, Object>> results =
        Assertions.assertDoesNotThrow(() -> drainAll(store.runGetQuery(query)));
    Assertions.assertEquals(1, results.size());
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testUnprojectedQueryUsesNonCoveringIndexViaHydration() {
    // A non-covering index (missing the declared column 'glitch') is still usable for an
    // unprojected query: it can evaluate the predicates (player, time are its key columns) and be
    // used for
    // ordering, and the missing columns -- plus any schema-less attribute -- are recovered by
    // hydrating each row from the base table.
    IndexMeta gsiPartial =
        globalIndex("gsiPartial", List.of("player", "time", "game"), List.of("score"));
    AliDocStore store = storeWithSchema(gsiPartial);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Assertions.assertEquals(
        "Index: gsiPartial",
        store.queryPlan(query),
        "a non-covering index that can evaluate the predicates is usable via hydration");

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("time", ColumnValue.fromString("2024-03-01")),
                new Column("glitch", ColumnValue.fromBoolean(true))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    List<Map<String, Object>> results = drainAll(store.runGetQuery(query));
    Assertions.assertEquals(1, results.size());
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testUnprojectedPartitionOnlyQueryUsesGlobalIndexAndHydrates() {
    // A partition-only unprojected query (equality on the index partition key, no order-by) routes
    // to the global index and hydrates from the base table, rather than falling back to a scan.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(true, gsi);

    Query query = new Query(store).where("player", FilterOperation.EQUAL, "mel");
    query.setFieldPaths(List.of()); // all fields

    Assertions.assertEquals(
        "Index: gsi",
        store.queryPlan(query),
        "partition-only unprojected query must use the global index, not a scan");

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("bonus", ColumnValue.fromLong(99))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    DocumentIterator iter = store.runGetQuery(query);
    iter.hasNext();
    ArgumentCaptor<GetRangeRequest> captor = ArgumentCaptor.forClass(GetRangeRequest.class);
    verify(syncClient, times(1)).getRange(captor.capture());
    Assertions.assertEquals(
        "gsi",
        captor.getValue().getRangeRowQueryCriteria().getTableName(),
        "the query must range over the global index, not the base table");
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testUnprojectedIndexQueryHydratesSchemalessAttribute() {
    // Core oracle: an unprojected query served from the index returns an index row lacking the
    // schema-less 'bonus'; hydration re-reads the full base row so the decoded document carries
    // bonus plus every key field, and the row exposed to pagination keeps the INDEX primary key.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    PrimaryKey indexKey = indexPk("mel", "2024-03-01", "game1");
    Row indexRow =
        new Row(
            indexKey,
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("glitch", ColumnValue.fromBoolean(false))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("time", ColumnValue.fromString("2024-03-01")),
                new Column("glitch", ColumnValue.fromBoolean(false)),
                new Column("bonus", ColumnValue.fromLong(99))));
    // A non-null continuation cursor keeps the iterator from reporting the range as fully drained,
    // so the pagination token reflects the last consumed row's key.
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), indexPk("z", "z", "z")));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    DocumentIterator iter = store.runGetQuery(query);
    Map<String, Object> doc = new HashMap<>();
    Assertions.assertTrue(iter.hasNext());
    iter.next(new Document(doc));

    Assertions.assertEquals(
        99L,
        ((Number) doc.get("bonus")).longValue(),
        "hydration must recover the schema-less attribute from the base row");
    Assertions.assertEquals("mel", doc.get("player"), "key field player must be present");
    Assertions.assertEquals("game1", doc.get("game"), "key field game must be present");

    PrimaryKey token = ((AliPaginationToken) iter.getPaginationToken()).getNextStartPrimaryKey();
    Assertions.assertEquals(
        indexKey, token, "the row exposed to pagination must carry the INDEX primary key");
  }

  @Test
  void testHydrationMissSkipsStaleIndexRow() {
    // A base row concurrently deleted after the index still lists it comes back from BatchGetRow
    // with a null row; hydration drops that stale index entry rather than emitting a phantom.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Row liveIndexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row staleIndexRow =
        new Row(
            indexPk("mel", "2024-04-01", "game2"),
            List.of(new Column("score", ColumnValue.fromLong(20))));
    Row liveBaseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("bonus", ColumnValue.fromLong(99))));
    // liveBaseRow present; the stale row's base read returns a null row (deleted concurrently).
    List<Row> baseRows = new ArrayList<>();
    baseRows.add(liveBaseRow);
    baseRows.add(null);
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(liveIndexRow, staleIndexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", baseRows));

    List<Map<String, Object>> results = drainAll(store.runGetQuery(query));
    Assertions.assertEquals(1, results.size(), "the stale index row must be dropped");
    Assertions.assertEquals("game1", results.get(0).get("game"));
  }

  @Test
  void testHydrationFailedSubRowIsNotSilentlyDropped() {
    // A base-row read that comes back FAILED (throttled / partial server error) after the client's
    // own request-level retries is NOT a deletion. Hydration must not silently omit it -- doing so
    // would return a short result set while the pagination cursor advances past the gap,
    // permanently skipping the row. When the bounded re-drive budget is exhausted, it must fail
    // loud with the concrete type AND retryability of the failed sub-row's error code (routed
    // through the driver's exception mapper), not a blanket non-retryable base exception.
    // OTSServerBusy maps to UnknownException, which is retryable.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    // Every BatchGetRow attempt returns the sub-row failed, so the bounded retries are exhausted.
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", "OTSServerBusy", 0));

    DocumentIterator iter = store.runGetQuery(query);
    UnknownException thrown =
        Assertions.assertThrows(
            UnknownException.class,
            () -> drainAll(iter),
            "an unrecoverable failed sub-row must fail loud as the mapped type, not be dropped");
    Assertions.assertTrue(
        thrown.isRetryable(),
        "OTSServerBusy is transient, so the exhaustion failure must be retryable");
    // A retryable per-row failure exhausts the full bounded budget: 1 initial read +
    // MAX_HYDRATION_RETRIES (3) re-drives before failing loud.
    verify(syncClient, times(4)).batchGetRow(any());
  }

  @Test
  void testHydrationExhaustionCarriesNonRetryableClassification() {
    // A NON-retryable per-row failure must fail loud IMMEDIATELY as its correct mapped type,
    // consuming zero re-drives: re-driving a permanent error cannot recover the row, it only burns
    // the bounded budget. OTSInvalidPK maps to InvalidArgumentException with isRetryable()==false;
    // the failure must surface as exactly that (not a mislabeled retryable exception) after a
    // SINGLE batchGetRow call.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", "OTSInvalidPK", 0));

    DocumentIterator iter = store.runGetQuery(query);
    InvalidArgumentException thrown =
        Assertions.assertThrows(InvalidArgumentException.class, () -> drainAll(iter));
    Assertions.assertFalse(
        thrown.isRetryable(),
        "OTSInvalidPK is a caller error, so the failure must be non-retryable");
    // Zero retries consumed: a non-retryable per-row failure fails fast on the initial response.
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testHydrationFailedSubRowRetriedThenSucceeds() {
    // A transient row-level failure that clears on retry must NOT drop the row: the bounded
    // re-drive recovers it and hydration returns the full base row.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("bonus", ColumnValue.fromLong(99))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    // First attempt: the sub-row fails; the retry succeeds and returns the base row.
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", "OTSServerBusy", 0))
        .thenReturn(batchGetResponse("scores", List.of(baseRow)));

    List<Map<String, Object>> results =
        Assertions.assertDoesNotThrow(() -> drainAll(store.runGetQuery(query)));
    Assertions.assertEquals(1, results.size(), "the retried row must be present, not dropped");
    Assertions.assertEquals(
        99L,
        ((Number) results.get(0).get("bonus")).longValue(),
        "hydration must recover the base row once the retry succeeds");
    verify(syncClient, times(2)).batchGetRow(any());
  }

  @Test
  void testHydrationMixedBatchFailsFastOnNonRetryableSubRow() {
    // A partial-failure batch that mixes a retryable sub-row (OTSServerBusy) with a non-retryable
    // one (OTSInvalidPK) must fail loud on the non-retryable one IMMEDIATELY -- the permanent error
    // can never clear on re-drive, so it surfaces as its own mapped type (non-retryable
    // InvalidArgumentException) after a single batchGetRow call, not after exhausting the budget.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    // Two index rows -> one hydration batch with two sub-rows (batchSize is 50).
    Row indexRow1 =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row indexRow2 =
        new Row(
            indexPk("mel", "2024-03-02", "game2"),
            List.of(new Column("score", ColumnValue.fromLong(20))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow1, indexRow2), null));
    // Sub-row 0 is retryable, sub-row 1 is non-retryable; the non-retryable one wins.
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", List.of("OTSServerBusy", "OTSInvalidPK")));

    DocumentIterator iter = store.runGetQuery(query);
    InvalidArgumentException thrown =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () -> drainAll(iter),
            "a non-retryable sub-row in a mixed batch must fail loud as its mapped type");
    Assertions.assertFalse(
        thrown.isRetryable(),
        "OTSInvalidPK is a caller error, so the failure must be non-retryable");
    // Failed fast on the initial response: no retries consumed despite a retryable sibling sub-row.
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testHydrationRetryRequestConstructionFailureSurfacesMapped() {
    // The retry-request builder (BatchGetRowRequest.createRequestForRetry) throws a raw
    // IllegalArgumentException when a failed sub-row's index has no matching criteria row. That
    // fires during iteration, outside Query.get()'s mapException boundary, so it must be routed
    // through the driver's exception mapper rather than escaping raw. A retryable sub-row (so the
    // re-drive is attempted) whose index (5) is absent from the single-row criteria triggers it;
    // IllegalArgumentException maps to InvalidArgumentException.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // unprojected -> hydrates via batchGetRow

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    // Retryable failure (so a re-drive is attempted) but at an index absent from the criteria (only
    // index 0 exists), so createRequestForRetry throws IllegalArgumentException.
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", "OTSServerBusy", 5));

    DocumentIterator iter = store.runGetQuery(query);
    InvalidArgumentException thrown =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () -> drainAll(iter),
            "a raw IllegalArgumentException from createRequestForRetry must surface as the mapped"
                + " type, not escape un-mapped");
    Assertions.assertFalse(thrown.isRetryable());
    // The failure happens while building the retry request, after the single initial read.
    verify(syncClient, times(1)).batchGetRow(any());
  }

  @Test
  void testHydrationPreservesIndexOrderAndPaginationToken() {
    // Output rows follow INDEX order regardless of the order BatchGetRow returns base rows, and the
    // pagination token is the last consumed row's INDEX primary key.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields

    PrimaryKey indexKey1 = indexPk("mel", "2024-03-01", "game1");
    PrimaryKey indexKey2 = indexPk("mel", "2024-04-01", "game2");
    Row indexRow1 = new Row(indexKey1, List.of(new Column("score", ColumnValue.fromLong(10))));
    Row indexRow2 = new Row(indexKey2, List.of(new Column("score", ColumnValue.fromLong(20))));
    Row baseRow1 =
        new Row(basePk("game1", "mel"), List.of(new Column("marker", ColumnValue.fromLong(1))));
    Row baseRow2 =
        new Row(basePk("game2", "mel"), List.of(new Column("marker", ColumnValue.fromLong(2))));
    // BatchGetRow returns the base rows in REVERSE order; hydration must still emit index order.
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow1, indexRow2), indexPk("z", "z", "z")));
    when(syncClient.batchGetRow(any()))
        .thenReturn(batchGetResponse("scores", List.of(baseRow2, baseRow1)));

    DocumentIterator iter = store.runGetQuery(query);
    Map<String, Object> doc1 = new HashMap<>();
    Map<String, Object> doc2 = new HashMap<>();
    Assertions.assertTrue(iter.hasNext());
    iter.next(new Document(doc1));
    Assertions.assertTrue(iter.hasNext());
    iter.next(new Document(doc2));

    Assertions.assertEquals(
        1L, ((Number) doc1.get("marker")).longValue(), "first output is the first index row");
    Assertions.assertEquals(
        2L, ((Number) doc2.get("marker")).longValue(), "second output is the second index row");
    PrimaryKey token = ((AliPaginationToken) iter.getPaginationToken()).getNextStartPrimaryKey();
    Assertions.assertEquals(indexKey2, token, "pagination token must be the last INDEX key");
  }

  @Test
  void testProjectedIndexQueryDoesNotHydrate() {
    // A projected index query trims to the projection and must NOT issue a base-table hydration.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of("score", "glitch")); // covered projection

    Assertions.assertEquals("Index: gsi", store.queryPlan(query));

    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(new ArrayList<>(), null));

    DocumentIterator iter = store.runGetQuery(query);
    iter.hasNext();
    verify(syncClient, never()).batchGetRow(any());
  }

  @Test
  void testUnprojectedPredicateFieldNotInIndexDoesNotUseIndex() {
    // Fail-closed routing: an index can serve a query only if it can evaluate every predicate. Here
    // the query filters on 'glitch', which the index does not carry, so the query must route to a
    // base-table scan, never the index (which could not apply the glitch filter).
    IndexMeta gsiPartial =
        globalIndex("gsiPartial", List.of("player", "time", "game"), List.of("score"));
    AliDocStore store = storeWithSchema(gsiPartial);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("glitch", FilterOperation.EQUAL, false);
    query.setFieldPaths(List.of()); // all fields

    Assertions.assertEquals(
        "Scan: scores",
        store.queryPlan(query),
        "a predicate on a field the index lacks must not be served from that index");
  }

  @Test
  void testRunHydratesRowsFromBaseTable() {
    // QueryRunner-level: with a base table configured, run() replaces each index row's columns with
    // the full base row read by its base primary key, recovering columns the index does not carry.
    PrimaryKey indexKey = indexPk("mel", "2024-03-01", "game1");
    Row indexRow = new Row(indexKey, List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(
            basePk("game1", "mel"),
            List.of(
                new Column("score", ColumnValue.fromLong(10)),
                new Column("bonus", ColumnValue.fromLong(99))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    QueryRunner runner =
        new QueryRunner(
            syncClient, "gsi", indexPk("mel", "", ""), indexPk("mel", "~", "~"),
            Direction.FORWARD, null, null, null, "scores", List.of("game", "player"), 50, 0,
            ali::mapException);
    List<Row> items = new ArrayList<>();
    runner.run(null, items);

    Assertions.assertEquals(1, items.size());
    Assertions.assertNotNull(
        items.get(0).getLatestColumn("bonus"), "base column recovered by hydration");
    Assertions.assertEquals(
        indexKey, items.get(0).getPrimaryKey(), "hydrated row keeps the INDEX primary key");
  }

  @Test
  void testRunHydrationPreservesPrimaryKey() {
    // QueryRunner-level: hydration swaps in the base row's columns but keeps the INDEX primary key,
    // so the pagination cursor derived from the returned row stays on the index.
    PrimaryKey indexKey = indexPk("mel", "2024-03-01", "game1");
    Row indexRow = new Row(indexKey, List.of(new Column("score", ColumnValue.fromLong(10))));
    Row baseRow =
        new Row(basePk("game1", "mel"), List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any())).thenReturn(batchGetResponse("scores", List.of(baseRow)));

    QueryRunner runner =
        new QueryRunner(
            syncClient, "gsi", indexPk("mel", "", ""), indexPk("mel", "~", "~"),
            Direction.FORWARD, null, null, null, "scores", List.of("game", "player"), 50, 0,
            ali::mapException);
    List<Row> items = new ArrayList<>();
    runner.run(null, items);

    Assertions.assertEquals(indexKey, items.get(0).getPrimaryKey());
  }

  @Test
  void testProjectedQueryStillUsesCoveringIndex() {
    // Guard against over-correction: the fail-closed rule applies only to UNPROJECTED queries. A
    // projected query whose fields are all physically present in an index must still select it.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of("score", "glitch")); // both physically in the index

    Assertions.assertEquals(
        "Index: gsi", store.queryPlan(query), "projected query covered by the index must use it");
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

  @Test
  void testHydrationMissingBaseKeyColumnFailsLoud() {
    // Tablestore folds the full base primary key into every index key, so a base-key column is
    // always expected on an index row's primary key. If one is somehow absent, deriving the base
    // key for hydration must fail loud with a SubstrateSdkException naming the missing column,
    // rather than NPE into an opaque UnknownException.
    PrimaryKey brokenIndexKey =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("game", PrimaryKeyValue.fromString("game1"))
            .build(); // missing the base-key column "player"
    Row indexRow = new Row(brokenIndexKey, List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));

    QueryRunner runner =
        new QueryRunner(
            syncClient, "gsi", indexPk("mel", "", ""), indexPk("mel", "~", "~"),
            Direction.FORWARD, null, null, null, "scores", List.of("game", "player"), 50, 0,
            ali::mapException);
    List<Row> items = new ArrayList<>();
    SubstrateSdkException thrown =
        Assertions.assertThrows(SubstrateSdkException.class, () -> runner.run(null, items));
    Assertions.assertTrue(
        thrown.getMessage().contains("player"),
        "the error must name the missing base-key column");
  }

  @Test
  void testGetRangeProviderExceptionSurfacesMappedAndRetryable() {
    // Query.get() maps only the iterator-construction path; the getRange RPC fires later during
    // iteration, outside that boundary. A raw provider exception from getRange must not escape
    // QueryRunner un-mapped -- it must be routed through the driver's exception mapper so it
    // surfaces as the correctly-typed and correctly-retryable multicloudj exception. OTSServerBusy
    // maps to UnknownException, which is retryable.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query = new Query(store).where("player", FilterOperation.EQUAL, "mel");
    query.setFieldPaths(List.of("score")); // projected -> no hydration, so getRange is the failure

    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenThrow(new TableStoreException("boom", "OTSServerBusy"));

    DocumentIterator iter = store.runGetQuery(query);
    UnknownException thrown =
        Assertions.assertThrows(
            UnknownException.class,
            iter::hasNext,
            "a getRange provider failure during iteration must surface as the mapped type");
    Assertions.assertTrue(
        thrown.isRetryable(), "OTSServerBusy maps to a retryable UnknownException");
  }

  @Test
  void testHydrationBatchGetRowRequestExceptionSurfacesMapped() {
    // The hydration batchGetRow RPC also fires during iteration, outside Query.get()'s mapException
    // boundary. A request-level provider exception (thrown by the initial batchGetRow call, not a
    // per-row failed RowResult) must be routed through the driver's exception mapper too, surfacing
    // as the mapped type rather than a raw provider exception. OTSServerBusy maps to a retryable
    // UnknownException.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // unprojected -> hydrates via batchGetRow

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any()))
        .thenThrow(new TableStoreException("boom", "OTSServerBusy"));

    DocumentIterator iter = store.runGetQuery(query);
    UnknownException thrown =
        Assertions.assertThrows(
            UnknownException.class,
            () -> drainAll(iter),
            "a request-level batchGetRow failure must surface as the mapped type");
    Assertions.assertTrue(
        thrown.isRetryable(), "OTSServerBusy maps to a retryable UnknownException");
  }

  @Test
  void testHydrationBatchGetRowRetryExceptionSurfacesMapped() {
    // The retry batchGetRow call (re-driving failed sub-rows) is wrapped as well: a request-level
    // provider exception thrown on the retry must also route through the exception mapper, not
    // escape raw. First attempt returns a failed sub-row; the re-drive call then throws.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // unprojected -> hydrates via batchGetRow

    Row indexRow =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow), null));
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", "OTSServerBusy", 0))
        .thenThrow(new TableStoreException("boom", "OTSServerBusy"));

    DocumentIterator iter = store.runGetQuery(query);
    UnknownException thrown =
        Assertions.assertThrows(
            UnknownException.class,
            () -> drainAll(iter),
            "a request-level failure on the retry batchGetRow must surface as the mapped type");
    Assertions.assertTrue(
        thrown.isRetryable(), "OTSServerBusy maps to a retryable UnknownException");
  }

  @Test
  void testIndexPlanningToleratesNullFieldPaths() {
    // setFieldPaths(null) is reachable via the public Lombok setter; planning must treat it like an
    // unprojected (empty-projection) query rather than NPE while collecting projected fields.
    Query nullPaths = new Query(ali).where("author", FilterOperation.EQUAL, "value");
    nullPaths.setFieldPaths(null);
    Query emptyPaths = new Query(ali).where("author", FilterOperation.EQUAL, "value");
    emptyPaths.setFieldPaths(List.of());

    wireMockClient();
    Assertions.assertEquals(
        "Index: global_index_3",
        Assertions.assertDoesNotThrow(() -> ali.queryPlan(nullPaths)),
        "null field paths must plan like an unprojected query, not NPE");
    Assertions.assertEquals(
        ali.queryPlan(emptyPaths),
        ali.queryPlan(nullPaths),
        "null field paths must behave identically to an empty projection");
  }

  @Test
  void testProjectedQueryTrimsNonProjectedPredicateFieldEndToEnd() {
    // End-to-end guard for the trimColumns CALL SITE in QueryRunner.run (the trimColumns helper is
    // unit-tested separately). A projected base-table query with a predicate on a NON-projected
    // field forces columns_to_get to fetch that predicate field (so the server-side filter can
    // evaluate), but the projection must trim it back out before the row reaches the caller. Drive
    // the whole path through runGetQuery / DocumentIterator.next: project 'price', filter on the
    // non-projected 'author', return a row carrying BOTH, and assert the decoded document exposes
    // 'price' but never leaks 'author'.
    Query query =
        new Query(ali)
            .where("title", FilterOperation.EQUAL, "value")
            .where("author", FilterOperation.GREATER_THAN, "x");
    query.setFieldPaths(List.of("price"));

    wireMockClient();
    PrimaryKey pk =
        PrimaryKeyBuilder.createPrimaryKeyBuilder()
            .addPrimaryKeyColumn("title", PrimaryKeyValue.fromString("value"))
            .addPrimaryKeyColumn("publisher", PrimaryKeyValue.fromString("WA"))
            .build();
    Row row =
        new Row(
            pk,
            List.of(
                new Column("price", ColumnValue.fromDouble(3.99)),
                new Column("author", ColumnValue.fromString("Neil"))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(row), null));

    DocumentIterator iter = ali.runGetQuery(query);
    Map<String, Object> doc = new HashMap<>();
    Assertions.assertTrue(iter.hasNext());
    iter.next(new Document(doc));

    Assertions.assertEquals(
        3.99, ((Number) doc.get("price")).doubleValue(), "the projected field must be returned");
    Assertions.assertNull(
        doc.get("author"),
        "a non-projected predicate field fetched only for the server-side filter must be trimmed,"
            + " not leaked to the caller");
  }

  @Test
  void testHydrationFetchesSecondBatchBeyondBatchSize() {
    // Hydration batches the base-table reads in chunks of batchSize (50). A page of 51 index rows
    // must therefore be hydrated with TWO BatchGetRow calls (50 + 1), and every row must survive:
    // the batch loop must not stop after the first chunk. Assert all 51 documents come back AND
    // that batchGetRow was called exactly twice.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields -> hydrate from base

    List<Row> indexRows = new ArrayList<>();
    List<Row> firstBatch = new ArrayList<>();
    List<Row> secondBatch = new ArrayList<>();
    for (int i = 0; i < 51; i++) {
      String game = "game" + i;
      indexRows.add(
          new Row(
              indexPk("mel", "2024-03-01", game),
              List.of(new Column("score", ColumnValue.fromLong(i)))));
      Row baseRow =
          new Row(
              basePk(game, "mel"),
              List.of(
                  new Column("score", ColumnValue.fromLong(i)),
                  new Column("bonus", ColumnValue.fromLong(i))));
      (i < 50 ? firstBatch : secondBatch).add(baseRow);
    }
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(indexRows, null));
    when(syncClient.batchGetRow(any()))
        .thenReturn(batchGetResponse("scores", firstBatch))
        .thenReturn(batchGetResponse("scores", secondBatch));

    List<Map<String, Object>> results = drainAll(store.runGetQuery(query));
    Assertions.assertEquals(
        51,
        results.size(),
        "every hydrated row must survive, including those past the first batch");
    verify(syncClient, times(2)).batchGetRow(any());
  }

  @Test
  void testHydrationExhaustionClassifiesNonRetryableInMixedFinalBatch() {
    // After the bounded re-drive budget (MAX_HYDRATION_RETRIES) is spent, the FINAL response's
    // failed sub-rows must be re-scanned for a non-retryable straggler before the generic
    // exhaustion throw. Here every attempt keeps failing: the first three responses carry two
    // RETRYABLE failures, and the final response mixes a retryable failure FIRST (OTSServerBusy)
    // with a non-retryable one LATER (OTSInvalidPK). The exhaustion throw maps only failed.get(0),
    // so
    // without the post-loop scan it would surface the retryable get(0) error; the post-loop scan
    // must classify the read by the non-retryable straggler instead. Assert the failure is the
    // non-retryable InvalidArgumentException, not a retryable UnknownException.
    IndexMeta gsi =
        globalIndex("gsi", List.of("player", "time", "game"), List.of("score", "glitch"));
    AliDocStore store = storeWithSchema(gsi);

    Query query =
        new Query(store)
            .where("player", FilterOperation.EQUAL, "mel")
            .where("time", FilterOperation.GREATER_THAN, "2024-02-01")
            .orderBy("time", true);
    query.setFieldPaths(List.of()); // all fields -> hydrate from base

    // Two index rows -> a single hydration batch with two sub-rows at indices 0 and 1, so the retry
    // request can re-issue both across the whole budget.
    Row indexRow1 =
        new Row(
            indexPk("mel", "2024-03-01", "game1"),
            List.of(new Column("score", ColumnValue.fromLong(10))));
    Row indexRow2 =
        new Row(
            indexPk("mel", "2024-03-02", "game2"),
            List.of(new Column("score", ColumnValue.fromLong(20))));
    when(syncClient.getRange(any(GetRangeRequest.class)))
        .thenReturn(getRangeResponse(List.of(indexRow1, indexRow2), null));
    // Initial read + the first two re-drives all fail retryably on both sub-rows; the final
    // re-drive (the fourth call, after the budget is spent) returns the mixed batch.
    when(syncClient.batchGetRow(any()))
        .thenReturn(failedBatchGetResponse("scores", List.of("OTSServerBusy", "OTSServerBusy")))
        .thenReturn(failedBatchGetResponse("scores", List.of("OTSServerBusy", "OTSServerBusy")))
        .thenReturn(failedBatchGetResponse("scores", List.of("OTSServerBusy", "OTSServerBusy")))
        .thenReturn(failedBatchGetResponse("scores", List.of("OTSServerBusy", "OTSInvalidPK")));

    DocumentIterator iter = store.runGetQuery(query);
    InvalidArgumentException thrown =
        Assertions.assertThrows(
            InvalidArgumentException.class,
            () -> drainAll(iter),
            "a non-retryable straggler in the exhausted final batch must classify the failure");
    Assertions.assertFalse(
        thrown.isRetryable(),
        "OTSInvalidPK is a caller error, so the exhaustion failure must be non-retryable");
    // 1 initial read + MAX_HYDRATION_RETRIES (3) re-drives = 4 calls before failing loud.
    verify(syncClient, times(4)).batchGetRow(any());
  }
}
