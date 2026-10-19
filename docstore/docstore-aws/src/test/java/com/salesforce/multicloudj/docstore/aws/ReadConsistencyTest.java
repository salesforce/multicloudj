package com.salesforce.multicloudj.docstore.aws;

import static com.salesforce.multicloudj.docstore.driver.FilterOperation.EQUAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.docstore.client.DocStoreClient;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.CollectionOptions;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.ReadOptions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

class ReadConsistencyTest {
  private static final ReadOptions STRONG = ReadOptions.builder().consistentRead(true).build();
  private static final ReadOptions DEFAULT = ReadOptions.builder().consistentRead(false).build();

  private static class Client extends DocStoreClient {
    Client(AwsDocStore store) {
      super(store);
    }
  }

  private AwsDocStore store(DynamoDbClient nativeClient, boolean consistentRead) {
    return new AwsDocStore.Builder()
        .withRegion("us-east-1")
        .withDDBClient(nativeClient)
        .withCollectionOptions(
            new CollectionOptions.CollectionOptionsBuilder()
                .withTableName("records")
                .withPartitionKey("pk")
                .withSortKey("sk")
                .withConsistentRead(consistentRead)
                .build())
        .build();
  }

  private Document document(String id) {
    return new Document(new HashMap<>(Map.of("pk", id, "sk", "version")));
  }

  @Test
  void keyReadsInheritAndOverrideWithoutChangingTheClient() {
    for (boolean clientDefault : new boolean[] {false, true}) {
      DynamoDbClient nativeClient = mock(DynamoDbClient.class);
      List<Boolean> flags = new ArrayList<>();
      when(nativeClient.batchGetItem(any(BatchGetItemRequest.class)))
          .thenAnswer(
              call -> {
                BatchGetItemRequest request = call.getArgument(0);
                flags.add(request.requestItems().get("records").consistentRead());
                return BatchGetItemResponse.builder().build();
              });
      try (AwsDocStore store = store(nativeClient, clientDefault)) {
        Client client = new Client(store);
        client.get(document("one"));
        client.get(document("one"), ReadOptions.builder().build());
        client.get(document("one"), STRONG, "pk");
        client.get(document("one"), DEFAULT);
        client.batchGet(List.of(document("one")), STRONG);
        client.batchGet(List.of(document("one")), DEFAULT);
        client.batchGet(List.of(document("one")));
        client.getActions().get(document("one"), STRONG).run();
        client.getActions().get(document("one"), DEFAULT).run();
        client.get(document("one"), (ReadOptions) null);
      }
      assertEquals(
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
    }
  }

  @Test
  void mixedConsistencyActionsAreSentInSeparateBatches() {
    DynamoDbClient nativeClient = mock(DynamoDbClient.class);
    when(nativeClient.batchGetItem(any(BatchGetItemRequest.class)))
        .thenReturn(BatchGetItemResponse.builder().build());
    try (AwsDocStore store = store(nativeClient, false)) {
      store.getActions().get(document("strong"), STRONG).get(document("normal"), DEFAULT).run();
      ArgumentCaptor<BatchGetItemRequest> requests =
          ArgumentCaptor.forClass(BatchGetItemRequest.class);
      verify(nativeClient, org.mockito.Mockito.times(2)).batchGetItem(requests.capture());
      Map<String, Boolean> flags = new HashMap<>();
      for (BatchGetItemRequest request : requests.getAllValues()) {
        var table = request.requestItems().get("records");
        assertEquals(1, table.keys().size());
        flags.put(table.keys().get(0).get("pk").s(), table.consistentRead());
      }
      assertEquals(Map.of("strong", true, "normal", false), flags);
    }
  }

  private Query query(AwsDocStore store, String partitionKey) {
    return new Query(store).where(partitionKey, EQUAL, "one");
  }

  @Test
  void queriesAndScansInheritAndOverrideClientConsistency() {
    DynamoDbClient nativeClient = mock(DynamoDbClient.class);
    when(nativeClient.describeTable(
            any(software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder().table(TableDescription.builder().build()).build());
    when(nativeClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
    try (AwsDocStore store = store(nativeClient, true)) {
      Query query = query(store, "pk");
      query.get(DEFAULT);
      assertFalse(Boolean.TRUE.equals(store.planQuery(query).getQueryRequest().consistentRead()));
      query.get();
      assertTrue(store.planQuery(query).getQueryRequest().consistentRead());
      query.get(ReadOptions.builder().build());
      assertTrue(store.planQuery(query).getQueryRequest().consistentRead());
      assertTrue(store.planQuery(new Query(store)).getScanRequest().consistentRead());
    }
    try (AwsDocStore store = store(nativeClient, false)) {
      Query query = query(store, "pk");
      query.get(STRONG);
      assertTrue(store.planQuery(query).getQueryRequest().consistentRead());
      query.get();
      assertFalse(Boolean.TRUE.equals(store.planQuery(query).getQueryRequest().consistentRead()));
    }
  }

  @Test
  void strongQueriesRejectGlobalIndexesButAllowLocalIndexes() {
    DynamoDbClient nativeClient = mock(DynamoDbClient.class);
    when(nativeClient.describeTable(
            any(software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder()
                .table(
                    TableDescription.builder()
                        .globalSecondaryIndexes(
                            GlobalSecondaryIndexDescription.builder()
                                .indexName("global")
                                .keySchema(
                                    KeySchemaElement.builder()
                                        .attributeName("gpk")
                                        .keyType(KeyType.HASH)
                                        .build())
                                .projection(
                                    Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build())
                        .localSecondaryIndexes(
                            LocalSecondaryIndexDescription.builder()
                                .indexName("local")
                                .keySchema(
                                    KeySchemaElement.builder()
                                        .attributeName("pk")
                                        .keyType(KeyType.HASH)
                                        .build(),
                                    KeySchemaElement.builder()
                                        .attributeName("lsk")
                                        .keyType(KeyType.RANGE)
                                        .build())
                                .projection(
                                    Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build())
                        .build())
                .build());
    when(nativeClient.query(any(QueryRequest.class))).thenReturn(QueryResponse.builder().build());
    try (AwsDocStore store = store(nativeClient, true)) {
      assertThrows(InvalidArgumentException.class, () -> query(store, "gpk").get());
      Query eventual = query(store, "gpk");
      eventual.get(DEFAULT);
      assertEquals("global", store.planQuery(eventual).getQueryRequest().indexName());
      assertFalse(
          Boolean.TRUE.equals(store.planQuery(eventual).getQueryRequest().consistentRead()));
      Query local = query(store, "pk").where("lsk", EQUAL, "value");
      local.get(STRONG);
      assertEquals("local", store.planQuery(local).getQueryRequest().indexName());
      assertTrue(store.planQuery(local).getQueryRequest().consistentRead());
    }
  }

  @Test
  void scanPagesRetainTheOverrideAndFollowingReadsInheritAgain() {
    DynamoDbClient nativeClient = mock(DynamoDbClient.class);
    when(nativeClient.describeTable(
            any(software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder().table(TableDescription.builder().build()).build());
    Map<String, AttributeValue> key =
        Map.of(
            "pk",
            AttributeValue.builder().s("one").build(),
            "sk",
            AttributeValue.builder().s("version").build());
    when(nativeClient.scan(any(ScanRequest.class)))
        .thenReturn(
            ScanResponse.builder().items(key).lastEvaluatedKey(key).build(),
            ScanResponse.builder().items(key).build(),
            ScanResponse.builder().build());
    try (AwsDocStore store = store(nativeClient, false)) {
      Query query = new Query(store);
      DocumentIterator iterator = query.get(STRONG);
      query.get();
      while (iterator.hasNext()) {
        iterator.next(new Document(new HashMap<>()));
      }
      ArgumentCaptor<ScanRequest> requests = ArgumentCaptor.forClass(ScanRequest.class);
      verify(nativeClient, org.mockito.Mockito.times(3)).scan(requests.capture());
      assertTrue(requests.getAllValues().get(0).consistentRead());
      assertFalse(Boolean.TRUE.equals(requests.getAllValues().get(1).consistentRead()));
      assertTrue(requests.getAllValues().get(2).consistentRead());
      assertEquals(key, requests.getAllValues().get(2).exclusiveStartKey());
    }
  }

  @Test
  void queryPaginationRetainsTheConsistencyOfItsExecution() {
    DynamoDbClient nativeClient = mock(DynamoDbClient.class);
    when(nativeClient.describeTable(
            any(software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest.class)))
        .thenReturn(
            DescribeTableResponse.builder().table(TableDescription.builder().build()).build());
    Map<String, AttributeValue> key =
        Map.of(
            "pk",
            AttributeValue.builder().s("one").build(),
            "sk",
            AttributeValue.builder().s("version").build());
    when(nativeClient.query(any(QueryRequest.class)))
        .thenReturn(
            QueryResponse.builder().items(key).lastEvaluatedKey(key).build(),
            QueryResponse.builder().items(key).build());
    try (AwsDocStore store = store(nativeClient, false)) {
      DocumentIterator iterator = query(store, "pk").get(STRONG);
      while (iterator.hasNext()) {
        iterator.next(new Document(new HashMap<>()));
      }
      ArgumentCaptor<QueryRequest> requests = ArgumentCaptor.forClass(QueryRequest.class);
      verify(nativeClient, org.mockito.Mockito.times(2)).query(requests.capture());
      assertTrue(requests.getAllValues().get(0).consistentRead());
      assertTrue(requests.getAllValues().get(1).consistentRead());
      assertEquals(key, requests.getAllValues().get(1).exclusiveStartKey());
    }
  }
}
