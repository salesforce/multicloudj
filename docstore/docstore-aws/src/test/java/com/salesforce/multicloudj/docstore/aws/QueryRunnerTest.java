package com.salesforce.multicloudj.docstore.aws;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueryRunnerTest {

    @Test
    void testQueryPlan() {
        DynamoDbClient ddb = mock(DynamoDbClient.class);
        ScanRequest scanRequest = mock(ScanRequest.class);
        QueryRequest queryRequest = mock(QueryRequest.class);

        QueryRunner runner = new QueryRunner(ddb, scanRequest, null, null);
        Assertions.assertEquals("Scan", runner.queryPlan());

        when(queryRequest.indexName()).thenReturn("index");
        runner = new QueryRunner(ddb, null, queryRequest, null);
        Assertions.assertEquals("Index index", runner.queryPlan());

        QueryRequest queryRequest2 = mock(QueryRequest.class);
        when(queryRequest2.tableName()).thenReturn(null);
        runner = new QueryRunner(ddb, null, queryRequest2, null);
        Assertions.assertEquals("Table", runner.queryPlan());
    }

    @Test
    void testRun() {
        DynamoDbClient ddb = mock(DynamoDbClient.class);

        ScanRequest mockScan = mock(ScanRequest.class);
        ScanRequest mockScan2 = mock(ScanRequest.class);
        QueryRunner runner = new QueryRunner(ddb, mockScan, null, null);
        ScanResponse mockScanResponse = mock(ScanResponse.class);
        ScanResponse mockScanResponse2 = mock(ScanResponse.class);
        ScanRequest.Builder mockBuilder = mock(ScanRequest.Builder.class);
        ScanRequest.Builder mockBuilder2 = mock(ScanRequest.Builder.class);

        when(mockScan.toBuilder()).thenReturn(mockBuilder);
        doAnswer((Answer<ScanRequest.Builder>) invocationOnMock -> {
            Map<String, AttributeValue> startAfter = invocationOnMock.getArgument(0);
            if (startAfter == null) {
                return mockBuilder;
            } else {
                return mockBuilder2;
            }
        }).when(mockBuilder).exclusiveStartKey(any());
        when(mockBuilder.build()).thenReturn(mockScan);
        when(mockBuilder2.build()).thenReturn(mockScan2);

        Map<String, AttributeValue> lastEvaluatedkey = new HashMap<>();
        lastEvaluatedkey.put(UUID.randomUUID().toString(), AttributeValue.builder().s(UUID.randomUUID().toString()).build());

        doReturn(null).when(mockScan).exclusiveStartKey();
        doReturn(lastEvaluatedkey).when(mockScan2).exclusiveStartKey();

        doAnswer((Answer<ScanResponse>) invocationOnMock -> {
            ScanRequest scanRequest = invocationOnMock.getArgument(0);
            if (scanRequest.exclusiveStartKey() == null || scanRequest.exclusiveStartKey().isEmpty()) {
                return mockScanResponse;
            } else {
                return mockScanResponse2;
            }
        }).when(ddb).scan(any(ScanRequest.class));
        when(mockScanResponse.lastEvaluatedKey()).thenReturn(lastEvaluatedkey);
        when(mockScanResponse2.lastEvaluatedKey()).thenReturn(null);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        Assertions.assertEquals(lastEvaluatedkey, runner.run(null, items, null));
        Assertions.assertNull(runner.run(lastEvaluatedkey, items, null));

        QueryRequest mockQuery = mock(QueryRequest.class);
        QueryRequest mockQuery2 = mock(QueryRequest.class);
        runner = new QueryRunner(ddb, null, mockQuery, null);
        QueryResponse mockQueryResponse = mock(QueryResponse.class);
        QueryResponse mockQueryResponse2 = mock(QueryResponse.class);
        QueryRequest.Builder mockQueryBuilder = mock(QueryRequest.Builder.class);
        QueryRequest.Builder mockQueryBuilder2 = mock(QueryRequest.Builder.class);
        when(mockQuery.toBuilder()).thenReturn(mockQueryBuilder);
        doAnswer((Answer<QueryRequest.Builder>) invocationOnMock -> {
            Map<String, AttributeValue> startAfter = invocationOnMock.getArgument(0);
            if (startAfter == null) {
                return mockQueryBuilder;
            } else {
                return mockQueryBuilder2;
            }
        }).when(mockQueryBuilder).exclusiveStartKey(any());
        when(mockQueryBuilder.build()).thenReturn(mockQuery);
        when(mockQueryBuilder2.build()).thenReturn(mockQuery2);
        doReturn(null).when(mockQuery).exclusiveStartKey();
        doReturn(lastEvaluatedkey).when(mockQuery2).exclusiveStartKey();
        doAnswer((Answer<QueryResponse>) invocationOnMock -> {
            QueryRequest queryRequest = invocationOnMock.getArgument(0);
            if (queryRequest.exclusiveStartKey() == null || queryRequest.exclusiveStartKey().isEmpty()) {
                return mockQueryResponse;
            } else {
                return mockQueryResponse2;
            }
        }).when(ddb).query(any(QueryRequest.class));
        when(mockQueryResponse.lastEvaluatedKey()).thenReturn(lastEvaluatedkey);
        when(mockQueryResponse2.lastEvaluatedKey()).thenReturn(null);
        Assertions.assertEquals(lastEvaluatedkey, runner.run(null, items, null));
        Assertions.assertNull(runner.run(lastEvaluatedkey, items, null));
    }

}
