package com.salesforce.multicloudj.docstore.aws;

import lombok.AllArgsConstructor;
import lombok.Getter;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

@AllArgsConstructor
@Getter
public class QueryRunner {

    private DynamoDbClient ddb;

    private ScanRequest scanRequest;

    private QueryRequest queryRequest;

    private Consumer<Predicate<Object>> beforeRun;

    public String queryPlan() {
        if (scanRequest != null) {
            return "Scan";
        }

        if (queryRequest.indexName() != null) {
            return String.format("Index %s", queryRequest.indexName());
        }

        return "Table";
    }

    public Map<String, AttributeValue> run(Map<String, AttributeValue> startAfter, List<Map<String, AttributeValue>> items, Function<Object, Boolean> asFunc) {
        if (scanRequest != null) {
            scanRequest = scanRequest.toBuilder().exclusiveStartKey(startAfter).build();
            if (beforeRun != null) {
                // TODO: add meaningful function.
            }

            ScanResponse response = ddb.scan(scanRequest);
            if (response == null) {
                throw new RuntimeException("Failed to scan");
            }

            items.addAll(response.items());
            return response.lastEvaluatedKey();
        } else {
            queryRequest = queryRequest.toBuilder().exclusiveStartKey(startAfter).build();
            if (beforeRun != null) {
                // TODO: add meaningful function.
            }
            QueryResponse response = ddb.query(queryRequest);
            if (response == null) {
                throw new RuntimeException("Failed to query");
            }

            items.addAll(response.items());
            return response.lastEvaluatedKey();
        }
    }
}
