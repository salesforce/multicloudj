package com.salesforce.multicloudj.docstore.aws;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.ReadOptions;
import com.salesforce.multicloudj.docstore.driver.testtypes.Book;
import com.salesforce.multicloudj.docstore.driver.testtypes.Person;
import java.io.IOException;
import java.io.NotSerializableException;
import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;

public class AwsDocStoreTest {
  private static final ReadOptions STRONG_READ = ReadOptions.builder().consistentRead(true).build();
  private static final ReadOptions NORMAL_READ =
      ReadOptions.builder().consistentRead(false).build();

  Person person =
      new Person(
          Collections.singleton("Jamie"),
          "Zoe",
          "Ford",
          Timestamp.newBuilder().setNanos(100).build());
  Book book =
      new Book(
          "YellowBook",
          person,
          "WA",
          Timestamp.newBuilder().setNanos(1000).build(),
          3.99f,
          new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
          null);

  static class TestDocStore extends AwsDocStore {}

  private static class TestDocStoreClient extends DocStoreClient {
    TestDocStoreClient(AwsDocStore store) {
      super(store);
    }
  }

  static class TestAction extends Action {
    public TestAction(
        ActionKind kind,
        Document document,
        List<String> fieldPaths,
        Map<String, Object> mods,
        boolean inAtomicWrites) {
      super(kind, document, fieldPaths, mods, inAtomicWrites);
    }

    public TestAction(
        ActionKind kind, Document document, List<String> fieldPaths, Map<String, Object> mods) {
      this(kind, document, fieldPaths, mods, false);
    }

    public void setKey(Object key) {
      this.key = key;
    }

    public void setIndex(int index) {
      this.index = index;
    }
  }

  private AwsDocStore docStore;
  DynamoDbClient mockedDDBClient;
  CollectionOptions collectionOptions;

  TableDescription tableDescription = null;

  @BeforeEach
  void setUp() {
    collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("test-chameleon")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(true)
            .withRevisionField("docRevision")
            .build();

    mockedDDBClient = mock(DynamoDbClient.class);
    when(mockedDDBClient.transactWriteItems(any(TransactWriteItemsRequest.class))).thenReturn(null);
    when(mockedDDBClient.batchGetItem(any(BatchGetItemRequest.class)))
        .thenReturn(mock(BatchGetItemResponse.class));

    docStore =
        new AwsDocStore.Builder()
            .withRegion("us-east-1")
            .withCollectionOptions(collectionOptions)
            .withDDBClient(mockedDDBClient)
            .build();

    KeySchemaElement partitionKey =
        KeySchemaElement.builder()
            .attributeName("title")
            .keyType(KeyType.HASH) // Partition Key
            .build();

    KeySchemaElement sortKey =
        KeySchemaElement.builder()
            .attributeName("publisher")
            .keyType(KeyType.RANGE) // Sort Key
            .build();

    AttributeDefinition partitionKeyAttr =
        AttributeDefinition.builder()
            .attributeName("title")
            .attributeType(ScalarAttributeType.S) // String type
            .build();

    AttributeDefinition sortKeyAttr =
        AttributeDefinition.builder()
            .attributeName("publisher")
            .attributeType(ScalarAttributeType.N) // Number type
            .build();

    AttributeDefinition lsiSortKeyAttr =
        AttributeDefinition.builder()
            .attributeName("LsiSortKey")
            .attributeType(ScalarAttributeType.S)
            .build();

    AttributeDefinition gsiHashKeyAttr =
        AttributeDefinition.builder()
            .attributeName("GsiHashKey")
            .attributeType(ScalarAttributeType.S)
            .build();

    AttributeDefinition gsiSortKeyAttr =
        AttributeDefinition.builder()
            .attributeName("GsiSortKey")
            .attributeType(ScalarAttributeType.S)
            .build();

    // Define Local Secondary Index
    LocalSecondaryIndexDescription lsi =
        LocalSecondaryIndexDescription.builder()
            .indexName("LSI_Index")
            .keySchema(
                KeySchemaElement.builder().attributeName("title").keyType(KeyType.HASH).build(),
                KeySchemaElement.builder()
                    .attributeName("LsiSortKey")
                    .keyType(KeyType.RANGE)
                    .build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .build();

    // Define Global Secondary Index
    GlobalSecondaryIndexDescription gsi =
        GlobalSecondaryIndexDescription.builder()
            .indexName("GSI_Index")
            .keySchema(
                KeySchemaElement.builder()
                    .attributeName("GsiHashKey")
                    .keyType(KeyType.HASH)
                    .build(),
                KeySchemaElement.builder()
                    .attributeName("GsiSortKey")
                    .keyType(KeyType.RANGE)
                    .build())
            .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
            .provisionedThroughput(
                ProvisionedThroughputDescription.builder()
                    .readCapacityUnits(5L)
                    .writeCapacityUnits(5L)
                    .build())
            .build();

    tableDescription =
        TableDescription.builder()
            .tableName("SampleTableWithIndexes")
            .tableStatus(TableStatus.ACTIVE)
            .creationDateTime(Instant.now())
            .keySchema(partitionKey, sortKey)
            .attributeDefinitions(
                partitionKeyAttr, sortKeyAttr, lsiSortKeyAttr, gsiHashKeyAttr, gsiSortKeyAttr)
            .localSecondaryIndexes(lsi)
            .globalSecondaryIndexes(gsi)
            .provisionedThroughput(
                ProvisionedThroughputDescription.builder()
                    .readCapacityUnits(5L)
                    .writeCapacityUnits(5L)
                    .build())
            .tableArn("arn:aws:dynamodb:us-east-1:123456789012:table/SampleTableWithIndexes")
            .build();
  }

  @AfterEach
  void tearDown() {
    if (docStore != null) {
      docStore.close();
    }
  }

  @Test
  void testDefaultConstructor() {
    AwsDocStore docStore = new AwsDocStore();
    Assertions.assertNotNull(docStore);

    AwsDocStore.Builder builder = docStore.builder();
    Assertions.assertNotNull(builder);
  }

  @Test
  void testMapException() {
    Assertions.assertInstanceOf(
        InvalidArgumentException.class,
        docStore.mapException(SdkClientException.create("Exception")));
    Assertions.assertInstanceOf(
        InvalidArgumentException.class, docStore.mapException(new IllegalArgumentException("")));
    Assertions.assertInstanceOf(
        UnknownException.class, docStore.mapException(new NotSerializableException("")));
  }

  @Test
  void testCreateDocStoreWithEndpoint() {
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("qzhouDS")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .build();

    docStore =
        new AwsDocStore.Builder()
            .withRegion("us-east-1")
            .withCollectionOptions(collectionOptions)
            .withEndpoint(URI.create("http://your-uri"))
            .build();

    Assertions.assertNotNull(docStore);
  }

  @Test
  void testProviderId() {
    Assertions.assertEquals("aws", docStore.getProviderId());
  }

  @Test
  void testExceptionHandling() {
    SdkClientException sdkClientException = SdkClientException.builder().build();
    Assertions.assertInstanceOf(
        InvalidArgumentException.class, docStore.mapException(sdkClientException));

    Assertions.assertInstanceOf(
        UnknownException.class, docStore.mapException(new IOException("Channel is closed")));
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
    AwsEncoder encoder = new AwsEncoder();
    doc.encode(encoder);
    AttributeValue av = encoder.getAttributeValue();
    Map<String, AttributeValue> map = av.m();
    Assertions.assertFalse(map.isEmpty());
  }

  @Test
  void testGetKey() {
    Document document = new Document(book);
    Assertions.assertEquals(
        "partitionKey:YellowBook,sortKey:WA", docStore.getKey(document).toString());
  }

  @Test
  void testCreate() {
    Document document = new Document(book);
    docStore.getActions().create(document).run();
  }

  @Test
  void testCreateWithTx() {
    Book book1 =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    Book book2 =
        new Book(
            "YellowBook",
            person,
            "CA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    Book book3 =
        new Book(
            "YellowBook",
            person,
            "TX",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    docStore
        .getActions()
        .create(new Document(book1))
        .enableAtomicWrites()
        .create(new Document(book2))
        .create(new Document(book3))
        .run();
    // Capture the TransactWriteItemsRequest passed to transactWriteItems
    ArgumentCaptor<TransactWriteItemsRequest> captor =
        ArgumentCaptor.forClass(TransactWriteItemsRequest.class);
    verify(mockedDDBClient).transactWriteItems(captor.capture());
    // Get the captured request
    TransactWriteItemsRequest capturedRequest = captor.getValue();
    Assertions.assertEquals(capturedRequest.transactItems().size(), 2);
    Assertions.assertEquals(
        "CA", capturedRequest.transactItems().get(0).put().item().get("publisher").s());
    Assertions.assertEquals(
        "TX", capturedRequest.transactItems().get(1).put().item().get("publisher").s());
  }

  @Test
  void testGet() {
    Book book = new Book("YellowBook", null, "WA", null, 3.99f, null, null);
    Document document = new Document(book);
    docStore.getActions().get(document, "price").run();
  }

  @Test
  void testDelete() {
    Book book = new Book("YellowBook", null, "WA", null, 3.99f, null, "something");
    Document document = new Document(book);
    docStore.getActions().delete(document).run();
  }

  @Test
  void testRunGets() {
    List<String> fp1 = new ArrayList<>(List.of("a.b", "a.c"));
    List<String> fp2 =
        new ArrayList<>(
            List.of("a.c", "a.b")); // Different order of fp1, treated as a different fp.
    List<String> fp3 = new ArrayList<>(List.of("d"));
    List<String> fp4 = new ArrayList<>(List.of("a.b", "c.d"));
    List<String> fp5 = new ArrayList<>(List.of("a"));

    List<Action> gets = new ArrayList<>();

    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp1, null);
    get1.setKey("partitionKey:YellowBook,sortKey:TX");
    TestAction get2 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp2, null);
    get2.setKey("partitionKey:YellowBook,sortKey:CA");
    TestAction get3 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp3, null);
    get3.setKey("partitionKey:YellowBook,sortKey:CA");
    TestAction get4 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp3, null);
    get4.setKey("partitionKey:YellowBook,sortKey:WA");
    TestAction get5 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp4, null);
    get5.setKey("partitionKey:YellowBook,sortKey:TX");
    TestAction get6 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp5, null);
    get6.setKey("partitionKey:YellowBook,sortKey:CA");

    gets.add(get1);
    gets.add(get2);
    gets.add(get3);
    gets.add(get4);
    gets.add(get5);
    gets.add(get6);
    Assertions.assertDoesNotThrow(() -> docStore.runGets(gets, null, 1));
  }

  @Test
  void testBatchGetWithMissingKeys() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    List<Action> gets = new ArrayList<>();
    List<String> fp1 = new ArrayList<>(List.of("d"));
    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), fp1, null);
    gets.add(get1);
    // Test when the key objects are missing.
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> docStore.batchGet(gets, null, 0, 0));
  }

  @Test
  void testBatchGetWithNoFieldPath() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    List<Action> gets = new ArrayList<>();

    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), null, null);
    get1.setKey("partitionKey:YellowBook,sortKey:WA");
    gets.add(get1);

    DynamoDbClient mockDdb = mock(DynamoDbClient.class);
    BatchGetItemResponse mockResponse = mock(BatchGetItemResponse.class);
    when(mockDdb.batchGetItem((BatchGetItemRequest) any())).thenReturn(mockResponse);
    Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("title", AttributeValue.builder().s("YellowBook").build());
    item.put("publisher", AttributeValue.builder().s("WA").build());
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("lastName", AttributeValue.builder().s("Ford").build());
    item.put("author", AttributeValue.builder().m(map).build());
    items.add(item);
    responses.put("table", items);

    when(mockResponse.responses()).thenReturn(responses);
    try {
      Field field = docStore.getClass().getDeclaredField("ddb");
      field.setAccessible(true);
      field.set(docStore, mockDdb);

    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    // Test do nothing when fp is missing.
    Assertions.assertDoesNotThrow(() -> docStore.batchGet(gets, null, 0, 0));
  }

  @Test
  void testBatchGet() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    List<Action> gets = new ArrayList<>();
    List<String> fp1 = new ArrayList<>(List.of("title", "publisher", "author.lastName"));
    TestAction get1 = new TestAction(ActionKind.ACTION_KIND_GET, new Document(book), fp1, null);
    get1.setKey("partitionKey:YellowBook,sortKey:WA");
    gets.add(get1);

    DynamoDbClient mockDdb = mock(DynamoDbClient.class);
    try {
      Field field = docStore.getClass().getDeclaredField("ddb");
      field.setAccessible(true);
      field.set(docStore, mockDdb);

    } catch (Exception e) {
      Assertions.fail("Failed to get field.", e);
    }

    // Test do nothing when fp is missing.

    BatchGetItemResponse mockResponse = mock(BatchGetItemResponse.class);
    when(mockDdb.batchGetItem((BatchGetItemRequest) any())).thenReturn(mockResponse);

    Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
    List<Map<String, AttributeValue>> items = new ArrayList<>();
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("title", AttributeValue.builder().s("YellowBook").build());
    item.put("publisher", AttributeValue.builder().s("WA").build());
    Map<String, AttributeValue> map = new HashMap<>();
    map.put("lastName", AttributeValue.builder().s("Ford").build());
    item.put("author", AttributeValue.builder().m(map).build());
    items.add(item);
    responses.put("table", items);

    when(mockResponse.responses()).thenReturn(responses);
    Assertions.assertDoesNotThrow(() -> docStore.batchGet(gets, null, 0, 0));
  }

  private AwsDocStore storeWithReadConsistency(boolean consistentRead) {
    return new AwsDocStore.Builder()
        .withRegion("us-east-1")
        .withDDBClient(mockedDDBClient)
        .withCollectionOptions(
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName(collectionOptions.getTableName())
                .withPartitionKey("title")
                .withSortKey("publisher")
                .withConsistentRead(consistentRead)
                .build())
        .build();
  }

  private Document keyDocument(String title) {
    return new Document(new HashMap<>(Map.of("title", title, "publisher", "version")));
  }

  private Map<String, AttributeValue> paginationKey() {
    return Map.of(
        "title", AttributeValue.builder().s("one").build(),
        "publisher", AttributeValue.builder().s("version").build());
  }

  @Test
  void testKeyReadsInheritAndOverrideReadConsistency() {
    for (boolean clientDefault : new boolean[] {false, true}) {
      List<Boolean> flags = new ArrayList<>();
      List<Integer> batchSizes = new ArrayList<>();
      when(mockedDDBClient.batchGetItem(any(BatchGetItemRequest.class)))
          .thenAnswer(
              call -> {
                BatchGetItemRequest request = call.getArgument(0);
                var table = request.requestItems().get(collectionOptions.getTableName());
                flags.add(table.consistentRead());
                batchSizes.add(table.keys().size());
                return BatchGetItemResponse.builder().build();
              });
      try (AwsDocStore store = storeWithReadConsistency(clientDefault)) {
        DocStoreClient client = new TestDocStoreClient(store);
        client.get(keyDocument("one"));
        client.get(keyDocument("one"), ReadOptions.builder().build());
        client.get(keyDocument("one"), STRONG_READ, "title");
        client.get(keyDocument("one"), NORMAL_READ);
        client.batchGet(List.of(keyDocument("one"), keyDocument("two")), STRONG_READ);
        client.batchGet(List.of(keyDocument("one"), keyDocument("two")), NORMAL_READ);
        client.batchGet(List.of(keyDocument("one"), keyDocument("two")));
        client.getActions().get(keyDocument("one"), STRONG_READ).run();
        client.getActions().get(keyDocument("one"), NORMAL_READ).run();
        client.get(keyDocument("one"), (ReadOptions) null);
      }
      Assertions.assertEquals(
          List.of(
              clientDefault,
              clientDefault,
              true,
              false,
              true,
              false,
              clientDefault,
              true,
              false,
              clientDefault),
          flags);
      Assertions.assertEquals(List.of(1, 1, 1, 1, 2, 2, 2, 1, 1, 1), batchSizes);
    }
  }

  @Test
  void testMixedReadConsistencyActionsUseSeparateBatches() {
    when(mockedDDBClient.batchGetItem(any(BatchGetItemRequest.class)))
        .thenReturn(BatchGetItemResponse.builder().build());
    docStore
        .getActions()
        .get(keyDocument("strong"), STRONG_READ)
        .get(keyDocument("normal"), NORMAL_READ)
        .run();
    ArgumentCaptor<BatchGetItemRequest> requests =
        ArgumentCaptor.forClass(BatchGetItemRequest.class);
    verify(mockedDDBClient, times(2)).batchGetItem(requests.capture());
    Map<String, Boolean> flags = new HashMap<>();
    for (BatchGetItemRequest request : requests.getAllValues()) {
      var table = request.requestItems().get(collectionOptions.getTableName());
      Assertions.assertEquals(1, table.keys().size());
      flags.put(table.keys().get(0).get("title").s(), table.consistentRead());
    }
    Assertions.assertEquals(Map.of("strong", true, "normal", false), flags);
  }

  @Test
  void testQueriesAndScansInheritAndOverrideReadConsistency() {
    when(mockedDDBClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(DescribeTableResponse.builder().table(tableDescription).build());
    when(mockedDDBClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().build());
    when(mockedDDBClient.scan(any(ScanRequest.class))).thenReturn(ScanResponse.builder().build());
    for (boolean clientDefault : new boolean[] {false, true}) {
      clearInvocations(mockedDDBClient);
      try (AwsDocStore store = storeWithReadConsistency(clientDefault)) {
        Query query = new Query(store).where("title", FilterOperation.EQUAL, "one");
        query.get().stop();
        query.consistentRead(!clientDefault).get().stop();
        query.get().stop();
        query.consistentRead(null).get().stop();
        new Query(store).get().stop();
        new Query(store).consistentRead(!clientDefault).get().stop();
      }
      ArgumentCaptor<QueryRequest> queries = ArgumentCaptor.forClass(QueryRequest.class);
      verify(mockedDDBClient, times(4)).query(queries.capture());
      Assertions.assertEquals(
          List.of(clientDefault, !clientDefault, !clientDefault, clientDefault),
          queries.getAllValues().stream()
              .map(request -> Boolean.TRUE.equals(request.consistentRead()))
              .toList());
      ArgumentCaptor<ScanRequest> scans = ArgumentCaptor.forClass(ScanRequest.class);
      verify(mockedDDBClient, times(2)).scan(scans.capture());
      Assertions.assertEquals(
          List.of(clientDefault, !clientDefault),
          scans.getAllValues().stream()
              .map(request -> Boolean.TRUE.equals(request.consistentRead()))
              .toList());
    }
  }

  @Test
  void testStrongQueriesIgnoreGlobalIndexesButAllowLocalIndexes() {
    when(mockedDDBClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(DescribeTableResponse.builder().table(tableDescription).build());
    when(mockedDDBClient.query(any(QueryRequest.class)))
        .thenReturn(QueryResponse.builder().build());
    try (AwsDocStore store = storeWithReadConsistency(true)) {
      new Query(store).where("GsiHashKey", FilterOperation.EQUAL, "one").get().stop();
      new Query(store)
          .where("GsiHashKey", FilterOperation.EQUAL, "one")
          .consistentRead(true)
          .get()
          .stop();
      new Query(store)
          .where("GsiHashKey", FilterOperation.EQUAL, "one")
          .where("GsiSortKey", FilterOperation.EQUAL, "version")
          .consistentRead(true)
          .get()
          .stop();
      new Query(store)
          .where("GsiHashKey", FilterOperation.EQUAL, "one")
          .consistentRead(false)
          .get()
          .stop();
      new Query(store)
          .where("title", FilterOperation.EQUAL, "one")
          .where("LsiSortKey", FilterOperation.EQUAL, "version")
          .consistentRead(true)
          .get()
          .stop();
    }
    ArgumentCaptor<QueryRequest> requests = ArgumentCaptor.forClass(QueryRequest.class);
    verify(mockedDDBClient, times(5)).query(requests.capture());
    Assertions.assertEquals(
        List.of("GSI_Index", "GSI_Index", "GSI_Index", "GSI_Index", "LSI_Index"),
        requests.getAllValues().stream().map(QueryRequest::indexName).toList());
    Assertions.assertEquals(
        List.of(false, false, false, false, true),
        requests.getAllValues().stream()
            .map(request -> Boolean.TRUE.equals(request.consistentRead()))
            .toList());
  }

  @Test
  void testScanPagesRetainConsistencyAfterQueryReset() {
    when(mockedDDBClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(DescribeTableResponse.builder().table(tableDescription).build());
    Map<String, AttributeValue> key = paginationKey();
    when(mockedDDBClient.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(key).lastEvaluatedKey(key).build(),
            ScanResponse.builder().build(),
            ScanResponse.builder().items(key).build());
    Query query = new Query(docStore);
    DocumentIterator iterator = query.consistentRead(true).get();
    query.consistentRead(null).get().stop();
    while (iterator.hasNext()) {
      iterator.next(new Document(new HashMap<>()));
    }
    iterator.stop();
    ArgumentCaptor<ScanRequest> requests = ArgumentCaptor.forClass(ScanRequest.class);
    verify(mockedDDBClient, times(3)).scan(requests.capture());
    Assertions.assertEquals(
        List.of(true, false, true),
        requests.getAllValues().stream()
            .map(request -> Boolean.TRUE.equals(request.consistentRead()))
            .toList());
    Assertions.assertEquals(key, requests.getAllValues().get(2).exclusiveStartKey());
  }

  @Test
  void testQueryPagesRetainReadConsistency() {
    when(mockedDDBClient.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(DescribeTableResponse.builder().table(tableDescription).build());
    Map<String, AttributeValue> key = paginationKey();
    when(mockedDDBClient.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder().items(key).lastEvaluatedKey(key).build(),
            QueryResponse.builder().items(key).build());
    DocumentIterator iterator =
        new Query(docStore).where("title", FilterOperation.EQUAL, "one").consistentRead(true).get();
    while (iterator.hasNext()) {
      iterator.next(new Document(new HashMap<>()));
    }
    iterator.stop();
    ArgumentCaptor<QueryRequest> requests = ArgumentCaptor.forClass(QueryRequest.class);
    verify(mockedDDBClient, times(2)).query(requests.capture());
    Assertions.assertEquals(
        List.of(true, true),
        requests.getAllValues().stream()
            .map(request -> Boolean.TRUE.equals(request.consistentRead()))
            .toList());
    Assertions.assertEquals(key, requests.getAllValues().get(1).exclusiveStartKey());
  }

  @Test
  void testNewWriteOperation() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            UUID.randomUUID().toString());
    TestAction get = new TestAction(ActionKind.ACTION_KIND_GET, new Document(person), null, null);
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> {
          docStore.newWriteOperation(get, null);
        });

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
  void testNewPutWithMissingKey() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    TestAction put = new TestAction(ActionKind.ACTION_KIND_PUT, new Document(person), null, null);

    Assertions.assertThrows(IllegalArgumentException.class, () -> docStore.newPut(put, null));
  }

  @Test
  void testNewPutWithCreate() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);

    Assertions.assertDoesNotThrow(() -> docStore.newPut(create, null));
  }

  @Test
  void testRunPut() {
    DynamoDbClient mockDdb = mock(DynamoDbClient.class);
    try {
      Field field = docStore.getClass().getDeclaredField("ddb");
      field.setAccessible(true);
      field.set(docStore, mockDdb);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    Book book =
        new Book(
            "YellowBook",
            person,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            3.99f,
            new HashMap<>(Map.of("Chapter 1", 5, "Chapter 2", 10)),
            null);
    AttributeValue av = AwsCodec.encodeDoc(new Document(book));
    TestAction create =
        new TestAction(ActionKind.ACTION_KIND_CREATE, new Document(book), null, null);

    Put put = Put.builder().tableName("table").item(av.m()).build();
    Map<String, String> expressionAttributeNames = new HashMap<>();
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    String precondition =
        docStore.buildPrecondition(create, expressionAttributeNames, expressionAttributeValues);
    if (precondition != null) {
      put =
          put.toBuilder()
              .conditionExpression(precondition)
              .expressionAttributeNames(expressionAttributeNames)
              .expressionAttributeValues(expressionAttributeValues)
              .build();
    }

    PutItemResponse mockResponse = mock(PutItemResponse.class);
    when(mockDdb.putItem((PutItemRequest) any())).thenReturn(mockResponse);
    Put rput = put;
    Assertions.assertDoesNotThrow(() -> docStore.runPut(rput, create, null));
  }

  @Test
  void testNewUpdate() {
    Assertions.assertDoesNotThrow(() -> docStore.newUpdate(null, null));
  }

  @Test
  void testNewDeleteWithMissingKey() {
    Person person =
        new Person(
            Collections.singleton("Jamie"),
            "Zoe",
            "Ford",
            Timestamp.newBuilder().setNanos(100).build());
    TestAction delete =
        new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(person), null, null);

    Assertions.assertThrows(IllegalArgumentException.class, () -> docStore.newDelete(delete, null));

    Book book =
        new Book(
            "YellowBook", null, null, Timestamp.newBuilder().setNanos(1000).build(), 0, null, null);
    TestAction delete2 =
        new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> docStore.newDelete(delete2, null));
  }

  @Test
  void testRunDelete() {
    DynamoDbClient mockDdb = mock(DynamoDbClient.class);
    try {
      Field field = docStore.getClass().getDeclaredField("ddb");
      field.setAccessible(true);
      field.set(docStore, mockDdb);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.", e);
    }

    Book book =
        new Book(
            "YellowBook",
            null,
            "WA",
            Timestamp.newBuilder().setNanos(1000).build(),
            0,
            null,
            UUID.randomUUID().toString());
    AttributeValue av =
        AwsCodec.encodeDocKeyFields(
            new Document(book),
            collectionOptions.getPartitionKey(),
            collectionOptions.getSortKey());
    TestAction deleteAction =
        new TestAction(ActionKind.ACTION_KIND_DELETE, new Document(book), null, null);

    Delete delete = Delete.builder().tableName("table").key(av.m()).build();
    Map<String, String> expressionAttributeNames = new HashMap<>();
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    String precondition =
        docStore.buildPrecondition(
            deleteAction, expressionAttributeNames, expressionAttributeValues);
    if (precondition != null) {
      delete =
          delete.toBuilder()
              .conditionExpression(precondition)
              .expressionAttributeNames(expressionAttributeNames)
              .expressionAttributeValues(expressionAttributeValues)
              .build();
    }

    Delete rdelete = delete;
    DeleteItemResponse mockResponse = mock(DeleteItemResponse.class);
    when(mockDdb.deleteItem((DeleteItemRequest) any())).thenReturn(mockResponse);
    Assertions.assertDoesNotThrow(() -> docStore.runDelete(rdelete, deleteAction, null));

    reset(mockDdb);
    when(mockDdb.deleteItem((DeleteItemRequest) any()))
        .thenThrow(
            ConditionalCheckFailedException.builder()
                .awsErrorDetails(
                    AwsErrorDetails.builder().errorCode("ConditionalCheckFailedException").build())
                .build());
    Assertions.assertThrows(
        ConditionalCheckFailedException.class,
        () -> docStore.runDelete(rdelete, deleteAction, null));

    reset(mockDdb);
    when(mockDdb.deleteItem((DeleteItemRequest) any())).thenThrow(DynamoDbException.class);
    Assertions.assertThrows(
        DynamoDbException.class, () -> docStore.runDelete(rdelete, deleteAction, null));
  }

  @Test
  void testGetTableDescription() {
    TestDocStore docStore = new TestDocStore();
    DynamoDbClient mockDdb = mock(DynamoDbClient.class);
    DescribeTableResponse mockDescribeTableResponse = mock(DescribeTableResponse.class);
    TableDescription mockTableDescription = mock(TableDescription.class);
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

      field = docStore.getClass().getSuperclass().getDeclaredField("ddb");
      field.setAccessible(true);
      field.set(docStore, mockDdb);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }
    when(mockDdb.describeTable(any(DescribeTableRequest.class)))
        .thenReturn(mockDescribeTableResponse);
    when(mockDescribeTableResponse.table()).thenReturn(mockTableDescription);
    Assertions.assertEquals(mockTableDescription, docStore.getTableDescription());
  }

  @Test
  void testKeyAttributes() throws InterruptedException {
    KeySchemaElement partitionKey =
        KeySchemaElement.builder()
            .attributeName("PartitionKeyName") // Replace with your partition key name
            .keyType(KeyType.HASH) // HASH for partition key
            .build();

    KeySchemaElement sortKey =
        KeySchemaElement.builder()
            .attributeName("SortKeyName") // Replace with your sort key name
            .keyType(KeyType.RANGE) // RANGE for sort key
            .build();

    List<KeySchemaElement> keySchemaElements = new ArrayList<>();
    keySchemaElements.add(partitionKey);
    keySchemaElements.add(sortKey);

    Assertions.assertNotNull(docStore.keyAttributes(keySchemaElements));
  }

  @Test
  void testHasFilter() {
    Assertions.assertFalse(docStore.hasFilter(null, ""));
    Query query = new Query(docStore).where("field", FilterOperation.EQUAL, "value");
    Assertions.assertFalse(docStore.hasFilter(query, "data"));
    Assertions.assertTrue(docStore.hasFilter(query, "field"));
    Assertions.assertTrue(docStore.hasEqualityFilter(query, "field"));
    query.where("size", FilterOperation.LESS_THAN, "5");
    Assertions.assertFalse(docStore.hasEqualityFilter(query, "size"));
  }

  @Test
  void testToKeyCondition() {
    Filter filter = new Filter("field", FilterOperation.EQUAL, "value");
    Map<String, String> expressionAttributeNames = new HashMap<>();
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();

    Assertions.assertNull(
        docStore.toKeyCondition(
            filter, expressionAttributeNames, expressionAttributeValues, "pkey", "skey"));
    String condition =
        docStore.toKeyCondition(
            filter, expressionAttributeNames, expressionAttributeValues, "field", "skey");
    Assertions.assertEquals("#attrfield = :value0", condition);
    Assertions.assertEquals("field", expressionAttributeNames.get("#attrfield"));
    Assertions.assertEquals("value", expressionAttributeValues.get(":value0").s());

    Assertions.assertEquals(
        "#attrfield < :value1",
        docStore.toKeyCondition(
            new Filter("field", FilterOperation.LESS_THAN, "value"),
            expressionAttributeNames,
            expressionAttributeValues,
            "field",
            "skey"));
    Assertions.assertEquals(
        "#attrfield > :value2",
        docStore.toKeyCondition(
            new Filter("field", FilterOperation.GREATER_THAN, "value"),
            expressionAttributeNames,
            expressionAttributeValues,
            "field",
            "skey"));
    Assertions.assertEquals(
        "#attrfield <= :value3",
        docStore.toKeyCondition(
            new Filter("field", FilterOperation.LESS_THAN_OR_EQUAL_TO, "value"),
            expressionAttributeNames,
            expressionAttributeValues,
            "field",
            "skey"));
    Assertions.assertEquals(
        "#attrfield >= :value4",
        docStore.toKeyCondition(
            new Filter("field", FilterOperation.GREATER_THAN_OR_EQUAL_TO, "value"),
            expressionAttributeNames,
            expressionAttributeValues,
            "field",
            "skey"));
  }

  @Test
  void testFiltersToCondition() {
    Filter filter1 = new Filter("fielda", FilterOperation.EQUAL, "valuea");
    Filter filter2 = new Filter("fieldb", FilterOperation.LESS_THAN, "valueb");
    Filter filter3 = new Filter("fieldc", FilterOperation.LESS_THAN_OR_EQUAL_TO, "valuec");
    Filter filter4 = new Filter("fieldd", FilterOperation.GREATER_THAN, "valued");
    Filter filter5 = new Filter("fielde", FilterOperation.GREATER_THAN_OR_EQUAL_TO, "valuee");
    Filter filter6 = new Filter("fieldf", FilterOperation.IN, List.of("valuef"));
    Filter filter7 = new Filter("fieldg", FilterOperation.NOT_IN, List.of("valueg"));
    Map<String, String> expressionAttributeNames = new HashMap<>();
    Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
    List<Filter> filters = new ArrayList<>();

    Assertions.assertThrows(
        IllegalArgumentException.class, () -> docStore.filtersToCondition(filters, null, null));
    filters.add(filter1);
    filters.add(filter2);
    filters.add(filter3);
    filters.add(filter4);
    filters.add(filter5);
    filters.add(filter6);
    filters.add(filter7);
    String condition =
        docStore.filtersToCondition(filters, expressionAttributeNames, expressionAttributeValues);
    Assertions.assertEquals(
        " #attrfielda = :value0 AND #attrfieldb < :value1 AND #attrfieldc <= :value2 AND"
            + " #attrfieldd > :value3 AND #attrfielde >= :value4 AND #attrfieldf IN (:value5) AND"
            + " NOT( #attrfieldg IN (:value6))",
        condition);
    Assertions.assertEquals("fielda", expressionAttributeNames.get("#attrfielda"));
    Assertions.assertEquals("valueb", expressionAttributeValues.get(":value1").s());
  }

  @Test
  void testRunGetQuery() {
    Query query = new Query(docStore).where("field", FilterOperation.EQUAL, "value");

    try {
      Field field = docStore.getClass().getDeclaredField("tableDescription");
      field.setAccessible(true);
      field.set(docStore, tableDescription);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    Assertions.assertThrows(RuntimeException.class, () -> docStore.runGetQuery(query));
  }

  @Test
  void testCheckPlan() {
    CollectionOptions collectionOptions =
        new CollectionOptions.CollectionOptionsBuilder()
            .withTableName("qzhouDS")
            .withPartitionKey("title")
            .withSortKey("publisher")
            .withAllowScans(false)
            .withRevisionField("docRevision")
            .build();

    AwsDocStore doc =
        new AwsDocStore.Builder()
            .withRegion("us-east-1")
            .withCollectionOptions(collectionOptions)
            .build();

    QueryRunner mockQueryRunner = mock(QueryRunner.class);
    ScanRequest scanRequest = mock(ScanRequest.class);
    when(mockQueryRunner.getScanRequest()).thenReturn(scanRequest);
    when(scanRequest.filterExpression()).thenReturn("Expression");
    Assertions.assertThrows(InvalidArgumentException.class, () -> doc.checkPlan(mockQueryRunner));

    QueryRunner mockGoodQueryRunner = mock(QueryRunner.class);
    ScanRequest scanRequestNoFilter = mock(ScanRequest.class);
    when(mockGoodQueryRunner.getScanRequest()).thenReturn(scanRequestNoFilter);
    when(scanRequestNoFilter.filterExpression()).thenReturn(null);
    Assertions.assertDoesNotThrow(() -> doc.checkPlan(mockGoodQueryRunner));
  }

  @Test
  void testPlanQuery() {
    try {
      Field field = docStore.getClass().getDeclaredField("tableDescription");
      field.setAccessible(true);
      field.set(docStore, tableDescription);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    Query query = new Query(docStore).where("field", FilterOperation.EQUAL, "value");
    QueryRunner queryRunner = docStore.planQuery(query);
    Assertions.assertNotNull(queryRunner.getScanRequest());

    int limit = 3;
    Query query2 =
        new Query(docStore).where("GsiHashKey", FilterOperation.EQUAL, "value").limit(limit);
    queryRunner = docStore.planQuery(query2);
    Assertions.assertNotNull(queryRunner.getQueryRequest());
    Assertions.assertEquals(limit, queryRunner.getQueryRequest().limit());

    Query query3 = new Query(docStore).where("field", FilterOperation.EQUAL, "value").limit(limit);
    query3.initGet(List.of("field.a", "field.b"));
    queryRunner = docStore.planQuery(query3);
    Assertions.assertNotNull(queryRunner.getScanRequest());
    Assertions.assertEquals(limit, queryRunner.getScanRequest().limit());
  }

  @Test
  void testGetBestQueryable() {
    try {
      Field field = docStore.getClass().getDeclaredField("tableDescription");
      field.setAccessible(true);
      field.set(docStore, tableDescription);
    } catch (Exception e) {
      Assertions.fail("Failed to get field.");
    }

    Query queryPkSk =
        new Query(docStore)
            .where("title", FilterOperation.EQUAL, "value")
            .where("publisher", FilterOperation.LESS_THAN, "value2");
    AwsDocStore.Queryable queryable = docStore.getBestQueryable(queryPkSk);
    Assertions.assertNotNull(queryable);
    Assertions.assertNotNull(queryable.getKey());
    Assertions.assertNull(queryable.getIndexName());

    Query queryLsi =
        new Query(docStore)
            .where("title", FilterOperation.EQUAL, "value")
            .where("LsiSortKey", FilterOperation.LESS_THAN, "value2");
    queryable = docStore.getBestQueryable(queryLsi);
    Assertions.assertNotNull(queryable);
    Assertions.assertNotNull(queryable.getKey());
    Assertions.assertNotNull(queryable.getIndexName());

    Query queryGsiPkSk =
        new Query(docStore)
            .where("GsiHashKey", FilterOperation.EQUAL, "value")
            .where("GsiSortKey", FilterOperation.LESS_THAN, "value2");
    queryable = docStore.getBestQueryable(queryGsiPkSk);
    Assertions.assertNotNull(queryable);
    Assertions.assertNotNull(queryable.getKey());
    Assertions.assertNotNull(queryable.getIndexName());

    Query queryPk = new Query(docStore).where("title", FilterOperation.EQUAL, "value");
    queryable = docStore.getBestQueryable(queryPk);
    Assertions.assertNotNull(queryable);
    Assertions.assertNotNull(queryable.getKey());
    Assertions.assertNull(queryable.getIndexName());

    Query queryGsiPk = new Query(docStore).where("GsiHashKey", FilterOperation.EQUAL, "value");
    queryable = docStore.getBestQueryable(queryGsiPk);
    Assertions.assertNotNull(queryable);
    Assertions.assertNotNull(queryable.getKey());
    Assertions.assertNotNull(queryable.getIndexName());
  }

  @Test
  void testGlobalFieldIncluded() {
    KeySchemaElement gsiPartitionKey =
        KeySchemaElement.builder()
            .attributeName("GsiHashKey")
            .keyType(KeyType.HASH) // Partition Key
            .build();

    KeySchemaElement gsiSortKey =
        KeySchemaElement.builder()
            .attributeName("GsiSortKey")
            .keyType(KeyType.RANGE) // Sort Key
            .build();

    Projection projection =
        Projection.builder()
            .projectionType(ProjectionType.INCLUDE) // INCLUDE specific attributes
            .nonKeyAttributes("SomeAttribute1", "SomeAttribute2")
            .build();

    ProvisionedThroughputDescription provisionedThroughput =
        ProvisionedThroughputDescription.builder()
            .readCapacityUnits(10L)
            .writeCapacityUnits(5L)
            .build();

    GlobalSecondaryIndexDescription gsi =
        GlobalSecondaryIndexDescription.builder()
            .indexName("SampleGSI")
            .keySchema(gsiPartitionKey, gsiSortKey)
            .projection(projection)
            .provisionedThroughput(provisionedThroughput)
            .build();

    Query query = new Query(docStore).where("GsiHashKey", FilterOperation.EQUAL, "value");
    Assertions.assertFalse(docStore.globalFieldIncluded(query, gsi));

    Query query2 = new Query(docStore).where("GsiHashKey", FilterOperation.EQUAL, "value");
    query2.initGet(List.of("SomeAttribute1", "SomeAttribute2"));
    Assertions.assertTrue(docStore.globalFieldIncluded(query2, gsi));
  }
}
