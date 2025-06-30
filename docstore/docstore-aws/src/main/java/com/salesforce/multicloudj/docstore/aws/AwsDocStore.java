package com.salesforce.multicloudj.docstore.aws;

import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.aws.CredentialsProvider;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.salesforce.multicloudj.docstore.aws.AwsCodec.decodeDoc;
import static com.salesforce.multicloudj.docstore.aws.AwsCodec.encodeValue;

@AutoService(AbstractDocStore.class)
public class AwsDocStore extends AbstractDocStore {

    private final int batchSize = 100;

    private DynamoDbClient ddb;

    private TableDescription tableDescription = null;

    public AwsDocStore() {
        super(new Builder());
    }

    public AwsDocStore(Builder builder) {
        super(builder);
        this.ddb = builder.ddb;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    private static class Key {
        private String partitionKey = null;
        private String sortKey = null;

        @Override
        public String toString() {
            return "partitionKey:" + partitionKey + "," + "sortKey:" + sortKey;
        }
    }

    @Override
    public Builder builder() {
        return new Builder();
    }

    @Override
    public Class<? extends SubstrateSdkException> getException(Throwable t) {
        // It's best to scan the stack list to some reasonable depth to find exceptions
        // we look for in certain order.
        // First check, if the exception being thrown is SubstrateSdkException, this means
        // the exception has already been converted to our Sdk exception.
        // Secondly, check the SdkClient exception and finally the exceptions from the server.
        Set<Throwable> exceptions = ExceptionUtils.getThrowableList(t).stream().limit(5).collect(Collectors.toSet());
        if (exceptions.stream().anyMatch(SubstrateSdkException.class::isInstance)) {
            // the exception is already mapped to internal, just let it flow
            return (Class<? extends SubstrateSdkException>) t.getClass();
        } else if (exceptions.stream().anyMatch(SdkClientException.class::isInstance) ||
                exceptions.stream().anyMatch(IllegalArgumentException.class::isInstance)) {
            return InvalidArgumentException.class;
        } else if (exceptions.stream().anyMatch(AwsServiceException.class::isInstance)) {
            AwsServiceException serviceException = (AwsServiceException) t;
            if (serviceException.awsErrorDetails() == null) {
                return UnknownException.class;
            }
            String errorCode = serviceException.awsErrorDetails().errorCode();
            return ErrorCodeMapping.getException(errorCode);
        }
        return UnknownException.class;
    }

    protected TableDescription getTableDescription() {
        if (tableDescription == null) {
            DescribeTableRequest describeTableRequest = DescribeTableRequest.builder()
                    .tableName(collectionOptions.getTableName())
                    .build();

            DescribeTableResponse describeTableResponse = ddb.describeTable(describeTableRequest);
            tableDescription = describeTableResponse.table();
        }

        return tableDescription;
    }

    @Override
    public Object getKey(Document document) {
        Key key = new Key();
        key.partitionKey = (String) document.getField(collectionOptions.getPartitionKey());
        if (key.partitionKey == null || key.partitionKey.isEmpty()) {
            throw new IllegalArgumentException("partitionKey cannot be null or empty");
        }

        if (collectionOptions.getSortKey() != null) {
            key.sortKey = (String) document.getField(collectionOptions.getSortKey());
        }

        return key;
    }

    // RunActions executes a slice of actions.
    // beforeDo is a callback which is called once before each action or action group.
    @Override
    public void runActions(List<Action> actions, Consumer<Predicate<Object>> beforeDo) {
        // Separate action lists for different operation types
        List<Action> beforeGets = new ArrayList<>();
        List<Action> getList = new ArrayList<>();
        List<Action> writeList = new ArrayList<>();
        List<Action> atomicWriteList = new ArrayList<>();
        List<Action> afterGets = new ArrayList<>();

        // Group actions based on their type
        Util.groupActions(actions, beforeGets, getList, writeList, atomicWriteList, afterGets);

        try {
            // Execute "beforeGets" actions
            runGets(beforeGets, beforeDo, batchSize);

            CompletableFuture<Void> writesTask = CompletableFuture.runAsync(() -> runWrites(writeList, beforeDo));
            CompletableFuture<Void> atomicWritesTask = CompletableFuture.runAsync(() -> runTxWrites(atomicWriteList, beforeDo));

            // Execute "getList" actions while writes are running
            runGets(getList, beforeDo, batchSize);

            // Wait for all concurrent  to complete
            writesTask.get();
            // wait for the atomic writes to complete
            atomicWritesTask.get();

            // Execute "afterGets" actions
            runGets(afterGets, beforeDo, batchSize);
        } catch (ExecutionException e) {
            if (e.getCause() == null) {
                throw new SubstrateSdkException(e);
            }
            throw (RuntimeException) e.getCause();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted while waiting for actions to complete.", e);
        }
    }

    protected void batchGet(List<Action> gets, Consumer<Predicate<Object>> beforeDo, int start, int end) {
        // AWS BatchGetItem expects a List<Map<String, AttributeValue>> where each Map represents one document's key
        List<Map<String, AttributeValue>> keysList = new ArrayList<>();
        for (int i = start; i <= end; i++) {
            AttributeValue av = AwsCodec.encodeDocKeyFields(gets.get(i).getDocument(), collectionOptions.getPartitionKey(), collectionOptions.getSortKey());
            if (av == null) {
                throw new IllegalArgumentException("Failed to encode keys.");
            }
            // Each document's key should be a separate map in the list
            keysList.add(av.m());
        }

        // Create the KeysAndAttributes.
        KeysAndAttributes.Builder keysAndAttributes = KeysAndAttributes.builder()
                .keys(keysList)
                .consistentRead(false);
        if (gets.get(start).getFieldPaths() != null && !gets.get(start).getFieldPaths().isEmpty()) {
            // Need to add the key fields if not included.
            boolean hasP = false;
            boolean hasS = false;
            StringBuilder projectionExpression = new StringBuilder();
            Map<String, String> expressionAttributeNames = new HashMap<>();

            for (String fp : gets.get(start).getFieldPaths()) {
                String[] fps = fp.split("\\.");

                String alias = "#attr" + expressionAttributeNames.size();
                expressionAttributeNames.put(alias, fps[0]);
                if (projectionExpression.length() > 0) {
                    projectionExpression.append(",");
                }

                String projection = alias;
                for (int i = 1; i < fps.length; i++) {
                    alias = "#attr" + expressionAttributeNames.size();
                    expressionAttributeNames.put(alias, fps[i]);
                    projection = projection + "." + alias;
                }
                projectionExpression.append(projection);

                if (fp.equals(collectionOptions.getPartitionKey())) {
                    hasP = true;
                }
                if (fp.equals(collectionOptions.getSortKey())) {
                    hasS = true;
                }
            }

            if (!hasP) {
                String pkAlias = "#attr" + expressionAttributeNames.size();
                expressionAttributeNames.put(pkAlias, collectionOptions.getPartitionKey());
                if (projectionExpression.length() > 0) {
                    projectionExpression.append(",");
                }
                projectionExpression.append(pkAlias);
            }

            if (!hasS && collectionOptions.getSortKey() != null) {
                String skAlias = "#attr" + expressionAttributeNames.size();
                expressionAttributeNames.put(skAlias, collectionOptions.getSortKey());
                if (projectionExpression.length() > 0) {
                    projectionExpression.append(",");
                }
                projectionExpression.append(skAlias);
            }

            // Create the KeysAndAttributes.
            keysAndAttributes = keysAndAttributes
                    .projectionExpression(projectionExpression.toString())
                    .expressionAttributeNames(expressionAttributeNames);


            if (beforeDo != null) {
                // TODO: Add meaningful function here.
            }
        }

        // Create RequestItems map.
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put(collectionOptions.getTableName(), keysAndAttributes.build());

        // Create the BatchGetItemRequest
        BatchGetItemRequest batchGetItemRequest = BatchGetItemRequest.builder().requestItems(requestItems).build();

        BatchGetItemResponse batchGetItemResponse = ddb.batchGetItem(batchGetItemRequest);
        Map<String, List<Map<String, AttributeValue>>> responses = batchGetItemResponse.responses();

        Map<String, Action> am = new HashMap<>();
        for (int i = start; i <= end; i++) {
            am.put(gets.get(i).getKey().toString(), gets.get(i));
        }

        if (responses.containsKey(collectionOptions.getTableName())) {
            List<Map<String, AttributeValue>> items = responses.get(collectionOptions.getTableName());

            // decode item in the list.
            for (Map<String, AttributeValue> item : items) {
                // Here first decode the key of an item. Then use the key the find the corresponding action.
                // Then decode the object and overwrite the doc in the action.
                Map<String, String> key = new HashMap<>();
                key.put(collectionOptions.getPartitionKey(), null);
                key.put(collectionOptions.getSortKey(), null);

                Document keyOnlyDoc = new Document(key);
                decodeDoc(AttributeValue.builder().m(item).build(), keyOnlyDoc);
                Object decKey = getKey(keyOnlyDoc);

                // There is a potential issue with the current docstore implementation if there are dup key objects.
                Action action = am.get(decKey.toString());
                decodeDoc(AttributeValue.builder().m(item).build(), action.getDocument());
            }
        }
    }

    private void runWrites(List<Action> writes, Consumer<Predicate<Object>> beforeDo) {
        if (writes.isEmpty()) {
            return;
        }
        List<WriteOperation> writeOperations = writes.stream()
                .map(action -> newWriteOperation(action, beforeDo))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

        // Submit each write operation as a CompletableFuture to the executor
        // Wait for all operations to complete
        CompletableFuture.allOf(writeOperations.stream()
                .map(op -> CompletableFuture.runAsync(op.getRun(), executorService))
                .toArray(CompletableFuture[]::new)).join();
        updateRevision(writeOperations);
    }

    private void runTxWrites(List<Action> writes, Consumer<Predicate<Object>> beforeDo) {
        if (writes.isEmpty()) {
            return;
        }
        List<WriteOperation> writeOperations = new ArrayList<>();
        List<TransactWriteItem> txWrites = new ArrayList<>();
        for (Action w : writes) {
            WriteOperation op = newWriteOperation(w, beforeDo);
            writeOperations.add(op);
            if (op != null) {
                txWrites.add(op.getWriteItem());
            }
        }
        TransactWriteItemsRequest request = TransactWriteItemsRequest.builder().transactItems(txWrites).clientRequestToken(Util.uniqueString()).build();
        ddb.transactWriteItems(request);
        updateRevision(writeOperations);
    }

    private void updateRevision(List<WriteOperation> writeOperations) {
        writeOperations.forEach(writeOperation -> {
            if (!StringUtils.isEmpty(writeOperation.getNewRevision())) {
                writeOperation
                        .getAction()
                        .getDocument()
                        .setField(getRevisionField(), writeOperation.getNewRevision());

            }
        });
    }

    protected WriteOperation newWriteOperation(Action action, Consumer<Predicate<Object>> beforeDo) {
        switch (action.getKind()) {
            case ACTION_KIND_CREATE:    // Fall through
            case ACTION_KIND_REPLACE:   // Fall through
            case ACTION_KIND_PUT:
                return newPut(action, beforeDo);
            case ACTION_KIND_UPDATE:
                return newUpdate(action, beforeDo);
            case ACTION_KIND_DELETE:
                return newDelete(action, beforeDo);
            default:
                throw new IllegalArgumentException("Unknown action: " + action);
        }
    }

    protected WriteOperation newPut(Action action, Consumer<Predicate<Object>> beforeDo) {
        AttributeValue av = AwsCodec.encodeDoc(action.getDocument());
        String mf = missingKeyField(av.m());
        if (action.getKind() != ActionKind.ACTION_KIND_CREATE && !mf.isEmpty()) {
            throw new IllegalArgumentException("Missing key field: " + mf);
        }

        String newPartitionKey = Util.uniqueString();
        if (mf.equals(collectionOptions.getPartitionKey())) {
            Map<String, AttributeValue> m = new HashMap<>();
            m.put(collectionOptions.getPartitionKey(), AttributeValue.builder().s(newPartitionKey).build());
            av = av.toBuilder().m(m).build();
        }
        if (collectionOptions.getSortKey() != null && mf.equals(collectionOptions.getSortKey())) {
            throw new IllegalArgumentException("Missing soft key: " + mf);
        }

        String rev = null;
        if (action.getDocument().hasField(getRevisionField())) {
            rev = Util.uniqueString();
            Map<String, AttributeValue> m = new HashMap<>(av.m());
            m.put(getRevisionField(), encodeValue(rev));
            av = av.toBuilder().m(m).build();
        }

        Put.Builder putBuilder = Put.builder()
                .tableName(collectionOptions.getTableName())
                .item(av.m());

        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        String precondition = buildPrecondition(action, expressionAttributeNames, expressionAttributeValues);
        if (precondition != null) {
            putBuilder = putBuilder.conditionExpression(precondition);
            if (!expressionAttributeNames.isEmpty()) {
                putBuilder = putBuilder.expressionAttributeNames(expressionAttributeNames);
            }
            if (!expressionAttributeValues.isEmpty()) {
                putBuilder = putBuilder.expressionAttributeValues(expressionAttributeValues);
            }
        }

        Put rput = putBuilder.build();
        return new WriteOperation(action,
                TransactWriteItem.builder().put(rput).build(),
                newPartitionKey,
                rev,
                () -> runPut(rput, action, beforeDo)
        );
    }

    protected void runPut(Put put, Action action, Consumer<Predicate<Object>> beforeDo) {
        PutItemRequest.Builder requestBuilder = PutItemRequest.builder()
                .tableName(put.tableName())
                .item(put.item());
        if (put.conditionExpression() != null) {
            requestBuilder.conditionExpression(put.conditionExpression());
        }
        if (put.expressionAttributeNames() != null && !put.expressionAttributeNames().isEmpty()) {
            requestBuilder.expressionAttributeNames(put.expressionAttributeNames());
        }
        if (put.expressionAttributeValues() != null && !put.expressionAttributeValues().isEmpty()) {
            requestBuilder.expressionAttributeValues(put.expressionAttributeValues());
        }

        PutItemRequest putItemRequest = requestBuilder.build();
        try {
            ddb.putItem(putItemRequest);
        } catch (ConditionalCheckFailedException e) {
            if (action.getKind() == ActionKind.ACTION_KIND_CREATE) {
                throw new ResourceAlreadyExistsException(e);
            } else {
                throw new ResourceNotFoundException(e);
            }
        }
    }

    private String missingKeyField(Map<String, AttributeValue> m) {
        if (m.get(collectionOptions.getPartitionKey()) == null) {
            return collectionOptions.getPartitionKey();
        } else if (collectionOptions.getSortKey() != null && m.get(collectionOptions.getSortKey()) == null) {
            return collectionOptions.getSortKey();
        } else {
            return "";
        }
    }

    protected String buildPrecondition(Action action, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        switch (action.getKind()) {
            case ACTION_KIND_CREATE:
                // Precondition: the document doesn't already exist.
                return "attribute_not_exists(" + collectionOptions.getPartitionKey() + ")";
            case ACTION_KIND_REPLACE:   // Fall through
            case ACTION_KIND_UPDATE:
                // Precondition: the revision matches, or if there is no revision, the document exists.
                String precondition = buildRevisionPrecondition(action.getDocument(), getRevisionField(), expressionAttributeNames, expressionAttributeValues);
                if (precondition == null) {
                    return "attribute_exists(" + collectionOptions.getPartitionKey() + ")";
                }
                return precondition;
            case ACTION_KIND_PUT:       // Fall through
            case ACTION_KIND_DELETE:
                // Precondition: the revision matches, if any.
                return buildRevisionPrecondition(action.getDocument(), getRevisionField(), expressionAttributeNames, expressionAttributeValues);
            case ACTION_KIND_GET:
                // No preconditions on a Get.
                return null;
            default:
                throw new IllegalArgumentException("Invalid action kind: " + action.getKind());
        }
    }

    private String buildRevisionPrecondition(Document doc, String revField, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        Object object = doc.getField(revField);
        if (object == null) {
            return null;
        }
        if (!(object instanceof String)) {
            throw new IllegalArgumentException(String.format("Invalid revision field %s type as %s, expect String type.", revField, object == null ? null : object.getClass().getName()));
        }
        String v = (String) doc.getField(revField);
        if (v == null || v.isEmpty()) {
            return null;
        }
        expressionAttributeNames.put("#revField", revField);
        expressionAttributeValues.put(":revValue", AttributeValue.builder().s(v).build());
        return "#revField = :revValue";
    }

    protected WriteOperation newUpdate(Action action, Consumer<Predicate<Object>> beforeDo) {
        // TODO:
        return null;
    }

    protected WriteOperation newDelete(Action action, Consumer<Predicate<Object>> beforeDo) {
        AttributeValue av = AwsCodec.encodeDocKeyFields(action.getDocument(), collectionOptions.getPartitionKey(), collectionOptions.getSortKey());
        if (av == null) {
            throw new IllegalArgumentException("Failed to encode keys.");
        }

        Delete delete = Delete.builder()
                .tableName(collectionOptions.getTableName())
                .key(av.m())
                .build();

        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        String precondition = buildPrecondition(action, expressionAttributeNames, expressionAttributeValues);
        if (precondition != null) {
            delete = delete.toBuilder().conditionExpression(precondition)
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();
        }

        Delete rdelete = delete;
        return new WriteOperation(action,
                TransactWriteItem.builder().delete(delete).build(),
                null,
                null,
                () -> runDelete(rdelete, action, beforeDo)
        );
    }

    protected void runDelete(Delete delete, Action action, Consumer<Predicate<Object>> beforeDo) {
        DeleteItemRequest.Builder requestBuilder = DeleteItemRequest.builder()
                .tableName(delete.tableName())
                .key(delete.key());
        if (delete.conditionExpression() != null) {
            requestBuilder.conditionExpression(delete.conditionExpression());
        }
        if (delete.expressionAttributeNames() != null && !delete.expressionAttributeNames().isEmpty()) {
            requestBuilder.expressionAttributeNames(delete.expressionAttributeNames());
        }
        if (delete.expressionAttributeValues() != null && !delete.expressionAttributeValues().isEmpty()) {
            requestBuilder.expressionAttributeValues(delete.expressionAttributeValues());
        }

        DeleteItemRequest deleteItemRequest = requestBuilder.build();
        ddb.deleteItem(deleteItemRequest);
    }

    // RunGetQuery executes a Query.
    @Override
    public DocumentIterator runGetQuery(Query query) {
        QueryRunner qr = planQuery(query);
        if (qr == null) {
            throw new SubstrateSdkException("Failed to get a query runner.");
        }

        checkPlan(qr);

        AwsDocumentIterator iter = new AwsDocumentIterator(
                qr,
                query,
                0);

        iter.run(query.getPaginationToken() != null ? ((AwsPaginationToken)query.getPaginationToken()).getExclusiveStartKey() : null);
        return iter;
    }

    protected void checkPlan(QueryRunner qr) {
        if (qr.getScanRequest() != null && qr.getScanRequest().filterExpression() != null && !collectionOptions.isAllowScans()) {
            throw new InvalidArgumentException("query requires a table scan; set Options.AllowScans to true to enable");
        }
    }

    @AllArgsConstructor
    @Getter
    static class Queryable {
        private String indexName;
        private Key key;
    }

    protected QueryRunner planQuery(Query query) {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        boolean hasProjection = false;

        StringBuilder projectionExpressionBuilder = new StringBuilder();
        if (query.getFieldPaths() != null && !query.getFieldPaths().isEmpty()) {
            for (String fieldPath : query.getFieldPaths()) {
                projectionExpressionBuilder.append("#").append(fieldPath).append(",");
                expressionAttributeNames.put("#" + fieldPath, fieldPath);
            }
            projectionExpressionBuilder.setLength(projectionExpressionBuilder.length() - 1);
            hasProjection = true;
        }

        // Find the best thing to query (table or index).
        Queryable queryable = getBestQueryable(query);

        // Collect keys required for pagination. If table is used, the list contains primary keys of base
        // table. If index is used, the list contains both base table primary keys and index primary keys.
        Set<String> paginationKeys = new HashSet<>();
        paginationKeys.add(collectionOptions.getPartitionKey());
        if (collectionOptions.getSortKey() != null) {
            paginationKeys.add(collectionOptions.getSortKey());
        }

        // queryable is not null for sure.
        if (queryable.indexName == null && queryable.key == null) {
            // No query can be done: fall back to scanning.
            if (query.getOrderByField() != null && !query.getOrderByField().isEmpty()) {
                // Scans are unordered, so we can't run this query.
                // TODO: If the user specifies all the partition keys, and there is a global
                // secondary index whose sort key is the order-by field, then we can query that index
                // for every value of the partition key and merge the results.
                // TODO: If the query has a reasonable limit N, then we can run a scan and keep
                // the top N documents in memory.
                throw new InvalidArgumentException("query requires a table scan, but has an ordering requirement; add an index or provide Options.RunQueryFallback");
            }

            ScanRequest.Builder scanRequestBuilder = ScanRequest.builder();
            if (query.getLimit() > 0) {
                scanRequestBuilder.limit(query.getLimit());
            }

            if (hasProjection) {
                scanRequestBuilder.projectionExpression(projectionExpressionBuilder.toString());
            }

            if (query.getFilters() != null && !query.getFilters().isEmpty()) {
                String filtersExpression = filtersToCondition(query.getFilters(), expressionAttributeNames, expressionAttributeValues);
                scanRequestBuilder.filterExpression(filtersExpression);
            }

            if (!expressionAttributeNames.isEmpty()) {
                scanRequestBuilder.expressionAttributeNames(expressionAttributeNames);
            }

            if (!expressionAttributeValues.isEmpty()) {
                scanRequestBuilder.expressionAttributeValues(expressionAttributeValues);
            }

            scanRequestBuilder.tableName(collectionOptions.getTableName());
            return new QueryRunner(ddb, scanRequestBuilder.build(), null, query.getBeforeQuery(), new ArrayList<>(paginationKeys));
        }

        // Do a query.
        QueryRequest.Builder queryRequestBuilder = QueryRequest.builder();
        if (query.getLimit() > 0) {
            queryRequestBuilder.limit(query.getLimit());
        }

        StringBuilder keyConditionBuilder = new StringBuilder();
        List<Filter> filtersNoKey = new ArrayList<>();
        for (Filter filter : query.getFilters()) {
            String condition = toKeyCondition(filter, expressionAttributeNames, expressionAttributeValues, queryable.getKey().getPartitionKey(), queryable.getKey().getSortKey());
            if (condition != null) {
                if (keyConditionBuilder.length() > 0) {
                    keyConditionBuilder.append(" AND ");
                }
                keyConditionBuilder.append(condition);
                continue;
            }
            filtersNoKey.add(filter);
        }

        if (keyConditionBuilder.length() > 0) {
            queryRequestBuilder.keyConditionExpression(keyConditionBuilder.toString());
        }

        // Add filter expression, which shouldn't contain PK and SK.
        if (!filtersNoKey.isEmpty()) {
            String filtersExpression = filtersToCondition(filtersNoKey, expressionAttributeNames, expressionAttributeValues);
            queryRequestBuilder.filterExpression(filtersExpression);
        }

        if (!expressionAttributeNames.isEmpty()) {
            queryRequestBuilder.expressionAttributeNames(expressionAttributeNames);
        }

        if (!expressionAttributeValues.isEmpty()) {
            queryRequestBuilder.expressionAttributeValues(expressionAttributeValues);
        }

        if (hasProjection) {
            queryRequestBuilder.projectionExpression(projectionExpressionBuilder.toString());
        }

        if (queryable.getIndexName() != null) {
            queryRequestBuilder.indexName(queryable.getIndexName());
        }

        queryRequestBuilder.tableName(collectionOptions.getTableName());
        if (query.getOrderByField() != null && !query.getOrderByField().isEmpty() && !query.isOrderAscending()) {
            queryRequestBuilder.scanIndexForward(query.isOrderAscending());
        }

        paginationKeys.add(queryable.getKey().getPartitionKey());
        if (queryable.getKey().getSortKey() != null) {
            paginationKeys.add(queryable.getKey().getSortKey());
        }

        return new QueryRunner(ddb, null, queryRequestBuilder.build(), query.getBeforeQuery(), new ArrayList<>(paginationKeys));
    }

    protected Queryable getBestQueryable(Query query) {
        // If the query has an "=" filter on the table's partition key, look at the table and local index.
        if (hasEqualityFilter(query, collectionOptions.getPartitionKey())) {
            // If the table has a sort key that's in the query, and the ordering
            // constraints works with the sort key, use the table.
            // (Query results are always ordered by the sort key.)
            if (hasFilter(query, collectionOptions.getSortKey()) && orderingConsistent(query, collectionOptions.getSortKey())) {
                return new Queryable(null, new Key(collectionOptions.getPartitionKey(), collectionOptions.getSortKey()));
            }

            // Check local indexes. They all have the same partition key as the base table.
            // If one has a sort key in the query, use it.
            if (getTableDescription().localSecondaryIndexes() != null) {
                for (LocalSecondaryIndexDescription li : getTableDescription().localSecondaryIndexes()) {
                    List<KeySchemaElement> keySchemaElements = li.keySchema();
                    Key key = keyAttributes(keySchemaElements);
                    if (hasFilter(query, key.getSortKey()) && localFieldIncluded(query, li) && orderingConsistent(query, key.getSortKey())) {
                        return new Queryable(li.indexName(), key);
                    }
                }
            }

        }

        // Check global indexes. If one has a matching partition and sort key, and
        // the projected fields of the index include those of the query, use it.
        if (getTableDescription().globalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndexDescription gi : getTableDescription().globalSecondaryIndexes()) {
                List<KeySchemaElement> keySchemaElements = gi.keySchema();
                Key key = keyAttributes(keySchemaElements);
                if (key == null || key.sortKey == null) {
                    continue; // We'll visit global indexes without a sort key later.
                }
                if (hasEqualityFilter(query, key.partitionKey) && hasFilter(query, key.sortKey) && globalFieldIncluded(query, gi) && orderingConsistent(query, key.sortKey)) {
                    return new Queryable(gi.indexName(), key);
                }
            }
        }


        // There are no matches for both partition and sort key. Now consider matches on partition key only.
        // That will still be better than a scan.
        // First, check the table itself.
        if (hasEqualityFilter(query, collectionOptions.getPartitionKey()) && orderingConsistent(query, collectionOptions.getSortKey())) {
            return new Queryable(null, new Key(collectionOptions.getPartitionKey(), collectionOptions.getSortKey()));
        }

        // No point checking local indexes: they have the same partition key as the table.
        // Check the global indexes.
        if (getTableDescription().globalSecondaryIndexes() != null) {
            for (GlobalSecondaryIndexDescription gi : getTableDescription().globalSecondaryIndexes()) {
                List<KeySchemaElement> keySchemaElements = gi.keySchema();
                Key key = keyAttributes(keySchemaElements);
                if (hasEqualityFilter(query, key.partitionKey) && globalFieldIncluded(query, gi) && orderingConsistent(query, key.sortKey)) {
                    return new Queryable(gi.indexName(), key);
                }
            }
        }

        // We cannot do a query.
        // TODO: return the reason why we couldn't. At a minimum, distinguish failure due to keys
        // from failure due to projection (i.e. a global index had the right partition and sort key,
        // but didn't project the necessary fields).
        return new Queryable(null, null);
    }

    // localFieldsIncluded reports whether a local index supports all the selected fields
    // of a query. Since DynamoDB will read explicitly provided fields from the table if
    // they are not projected into the index, the only case where a local index cannot
    // be used is when the query wants all the fields, and the index projection is not ALL.
    // See https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/LSI.html#LSI.Projections.
    private boolean localFieldIncluded(Query query, LocalSecondaryIndexDescription li) {
        return !query.getFieldPaths().isEmpty() || li.projection().projectionType() == ProjectionType.ALL;
    }

    // orderingConsistent reports whether the ordering constraint is consistent with the sort key field.
    // That is, either there is no OrderBy clause, or the clause specifies the sort field.
    private boolean orderingConsistent(Query query, String sortedField) {
        return query.getOrderByField() == null || query.getOrderByField().isEmpty() || query.getOrderByField().equals(sortedField);
    }

    // globalFieldsIncluded reports whether the fields selected by the query are
    // projected into (that is, contained directly in) the global index. We need this
    // check before using the index, because if a global index doesn't have all the
    // desired fields, then a separate RPC for each returned item would be necessary to
    // retrieve those fields, and we'd rather scan than do that.
    protected boolean globalFieldIncluded(Query query, GlobalSecondaryIndexDescription gi) {
        Projection proj = gi.projection();
        if (proj.projectionType() == ProjectionType.ALL) {
            // The index has all the fields of the table.
            return true;
        }

        if (query.getFieldPaths() == null || query.getFieldPaths().isEmpty()) {
            // The query wants all the fields of the table
            return false;
        }

        // The table's keys and the index's keys are always in the index.
        Key key = keyAttributes(gi.keySchema());
        Map<String, Boolean> indexFields = new HashMap<>();
        indexFields.put(collectionOptions.getPartitionKey(), true);
        indexFields.put(key.partitionKey, true);
        if (collectionOptions.getSortKey() != null) {
            indexFields.put(collectionOptions.getSortKey(), true);
        }
        if (key.sortKey != null) {
            indexFields.put(key.sortKey, true);
        }
        for (String nka : proj.nonKeyAttributes()) {
            indexFields.put(nka, true);
        }
        // Check every field path in the query must be in the index.
        for (String fp : query.getFieldPaths()) {
            if (!indexFields.containsKey(fp)) {
                return false;
            }
        }
        return true;
    }

    // Extract the names of the partition and sort key attributes from the schema of a
    // table or index.
    protected Key keyAttributes(List<KeySchemaElement> keySchemaElements) {
        Key key = new Key(null, null);
        for (KeySchemaElement keySchemaElement : keySchemaElements) {
            switch (keySchemaElement.keyType()) {
                case HASH:
                    key.setPartitionKey(keySchemaElement.attributeName());
                    break;
                case RANGE:
                    key.setSortKey(keySchemaElement.attributeName());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid key type: " + keySchemaElement.keyType());
            }
        }
        return key.getPartitionKey() != null ? key : null;
    }

    // Reports whether query has a filter that mentions the top-level field.
    protected boolean hasFilter(Query query, String field) {
        if (field.isEmpty()) {
            return false;
        }
        for (Filter filter : query.getFilters()) {
            if (filter.getFieldPath().equals(field)) {
                return true;
            }
        }
        return false;
    }

    // Reports whether query has a filter that checks if the top-level field is equal to something.
    protected boolean hasEqualityFilter(Query query, String field) {
        for (Filter filter : query.getFilters()) {
            if (filter.getOp() == FilterOperation.EQUAL && filter.getFieldPath().equals(field)) {
                return true;
            }
        }
        return false;
    }

    protected String toKeyCondition(Filter filter, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues, String pkey, String skey) {
        if (filter.getFieldPath().equals(pkey) || filter.getFieldPath().equals(skey)) {
            expressionAttributeNames.put("#attr" + filter.getFieldPath(), filter.getFieldPath());
            String valueKey = ":value" + expressionAttributeValues.size();
            expressionAttributeValues.put(valueKey, encodeValue(filter.getValue()));
            switch (filter.getOp()) {
                case EQUAL:
                    return "#attr" + filter.getFieldPath() + " = " + valueKey;
                case LESS_THAN:
                    return "#attr" + filter.getFieldPath() + " < " + valueKey;
                case GREATER_THAN:
                    return "#attr" + filter.getFieldPath() + " > " + valueKey;
                case LESS_THAN_OR_EQUAL_TO:
                    return "#attr" + filter.getFieldPath() + " <= " + valueKey;
                case GREATER_THAN_OR_EQUAL_TO:
                    return "#attr" + filter.getFieldPath() + " >= " + valueKey;
                default:
                    throw new IllegalArgumentException("Invalid filter operation: " + filter.getOp());
            }
        }
        return null;
    }

    protected String filtersToCondition(List<Filter> filters, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        if (filters.isEmpty()) {
            throw new IllegalArgumentException("No filters specified");
        }

        StringBuilder filtersExpressionBuilder = new StringBuilder();
        for (Filter filter : filters) {
            String filterExpression = filterToCondition(filter, expressionAttributeNames, expressionAttributeValues);
            if (filtersExpressionBuilder.length() > 0) {
                filtersExpressionBuilder.append(" AND");
            }
            filtersExpressionBuilder.append(filterExpression);
        }
        return filtersExpressionBuilder.toString();
    }

    private String filterToCondition(Filter filter, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues) {
        expressionAttributeNames.put("#attr" + filter.getFieldPath(), filter.getFieldPath());
        // Expression attribute name is fine to override, but expression attribute value shouldn't override.
        String valueKey = ":value" + expressionAttributeValues.size();
        expressionAttributeValues.put(valueKey, encodeValue(filter.getValue()));
        switch (filter.getOp()) {
            case EQUAL:
                return " #attr" + filter.getFieldPath() + " = " + valueKey;
            case LESS_THAN:
                return " #attr" + filter.getFieldPath() + " < " + valueKey;
            case GREATER_THAN:
                return " #attr" + filter.getFieldPath() + " > " + valueKey;
            case LESS_THAN_OR_EQUAL_TO:
                return " #attr" + filter.getFieldPath() + " <= " + valueKey;
            case GREATER_THAN_OR_EQUAL_TO:
                return " #attr" + filter.getFieldPath() + " >= " + valueKey;
            case IN:
                return toInOrNotInCondition(filter, expressionAttributeNames, expressionAttributeValues, true);
            case NOT_IN:
                return toInOrNotInCondition(filter, expressionAttributeNames, expressionAttributeValues, false);
            default:
                throw new IllegalArgumentException("Invalid filter operation: " + filter.getOp());
        }
    }

    // The expressions for IN are of the format: #attrPlayer IN (:value0, :value1) where :value_i is AttributeValue
    // defined in expressionAttributeValues map.
    // The expressions for NOT IN are of format same as IN but wrapped in NOT, for example: NOT (#attrPlayer IN (:value0, :value1))
    private String toInOrNotInCondition(Filter filter, Map<String, String> expressionAttributeNames, Map<String, AttributeValue> expressionAttributeValues, boolean in) {
        // remove the last AttributeValue added
        expressionAttributeValues.remove(":value" + (expressionAttributeValues.size()-1));
        expressionAttributeNames.put("#attr" + filter.getFieldPath(), filter.getFieldPath());
        try {
            Collection<Object> collection = (Collection<Object>) filter.getValue();
            StringBuilder filterExpressionBuilder = new StringBuilder();
            if (!in) {
                filterExpressionBuilder.append(" NOT").append("(");
            }
            filterExpressionBuilder.append(" #attr").append(filter.getFieldPath());
            filterExpressionBuilder.append(" IN ");
            filterExpressionBuilder.append("(");
            for (Object object : collection) {
                String valueKey = ":value" + expressionAttributeValues.size();
                filterExpressionBuilder.append(valueKey).append(", ");
                expressionAttributeValues.put(valueKey, encodeValue(object));
            }
            filterExpressionBuilder.setLength(filterExpressionBuilder.length() - 2);
            filterExpressionBuilder.append(")");
            if (!in) {
                filterExpressionBuilder.append(")");
            }
            return filterExpressionBuilder.toString();
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("Filter value is not collection type: " + filter.getFieldPath());
        }
    }

    // QueryPlan returns the plan for the query.
    @Override
    public String queryPlan(Query query) {
        QueryRunner qr = planQuery(query);
        return qr.queryPlan();
    }

    // Close cleans up any resources used by the Collection.
    @Override
    public void close() {
        this.executorService.shutdown();
        this.ddb.close();
    }

    public static class Builder extends AbstractDocStore.Builder<AwsDocStore, Builder> {
        private DynamoDbClient ddb;

        public Builder() {
            providerId("aws");
        }

        private static DynamoDbClient buildDDBClient(Builder builder) {
            DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder().region(Region.of(builder.getRegion()));

            AwsCredentialsProvider credentialsProvider = CredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), Region.of(builder.getRegion()));
            if (credentialsProvider != null) {
                clientBuilder.credentialsProvider(credentialsProvider);
            }

            if (builder.getEndpoint() != null) {
                clientBuilder.endpointOverride(builder.getEndpoint());
            }

            return clientBuilder.build();
        }

        @Override
        public Builder self() {
            return this;
        }

        public Builder withDDBClient(DynamoDbClient ddb) {
            this.ddb = ddb;
            return this;
        }

        @Override
        public AwsDocStore build() {
            if (ddb == null) {
                ddb = buildDDBClient(this);
            }
            return new AwsDocStore(this);
        }
    }
}