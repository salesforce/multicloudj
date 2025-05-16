package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.search.ParallelScanRequest;
import com.alicloud.openservices.tablestore.model.search.ParallelScanResponse;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.salesforce.multicloudj.docstore.ali.AliDocumentIterator.INIT_TOKEN;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryRunnerTest {

    @Test
    void testQueryPlan() {
        SyncClient tsClient = mock(SyncClient.class);
        ParallelScanRequest parallelScanRequest = mock(ParallelScanRequest.class);
        SQLQueryRequest queryRequest = mock(SQLQueryRequest.class);

        QueryRunner runner = new QueryRunner(tsClient, null, true, null);
        Assertions.assertEquals("Scan", runner.queryPlan());

        runner = new QueryRunner(tsClient, queryRequest, true, null);
        Assertions.assertEquals("Scan", runner.queryPlan());
    }

    @Test
    void testRun() {
        SyncClient tsClient = mock(SyncClient.class);
        SQLQueryRequest mockQuery = mock(SQLQueryRequest.class);
        QueryRunner runner = new QueryRunner(tsClient, mockQuery, true , null);
        SQLQueryResponse mockQueryResponse = mock(SQLQueryResponse.class);
        when(mockQueryResponse.getSQLResultSet()).thenReturn(new TestSQLResultSet());
        when(mockQueryResponse.getNextSearchToken()).thenReturn("testToken");
        when(tsClient.sqlQuery(any(SQLQueryRequest.class))).thenReturn(mockQueryResponse);
        List<Map<String, Object>> itemsQueryResponse = new ArrayList<>();
        Assertions.assertEquals("testToken", runner.run(null, null,itemsQueryResponse, null));
        Assertions.assertEquals(2, itemsQueryResponse.size());
    }
}
