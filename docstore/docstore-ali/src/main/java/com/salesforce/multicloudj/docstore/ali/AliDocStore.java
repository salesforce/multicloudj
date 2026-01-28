package com.salesforce.multicloudj.docstore.ali;


import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.CommitTransactionRequest;
import com.alicloud.openservices.tablestore.model.Condition;
import com.alicloud.openservices.tablestore.model.DeleteRowRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableRequest;
import com.alicloud.openservices.tablestore.model.DescribeTableResponse;
import com.alicloud.openservices.tablestore.model.IndexMeta;
import com.alicloud.openservices.tablestore.model.IndexType;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.PutRowRequest;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowExistenceExpectation;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionRequest;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.alicloud.openservices.tablestore.model.sql.SQLQueryRequest;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.hbr.model.v20170908.CreateRestoreJobRequest;
import com.aliyuncs.hbr.model.v20170908.CreateRestoreJobResponse;
import com.aliyuncs.hbr.model.v20170908.DescribeRestoreJobs2Request;
import com.aliyuncs.hbr.model.v20170908.DescribeRestoreJobs2Response;
import com.aliyuncs.profile.DefaultProfile;
import com.google.auto.service.AutoService;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.salesforce.multicloudj.docstore.ali.AliCodec.decodeDoc;
import static com.salesforce.multicloudj.docstore.ali.ErrorCodeMapping.OTS_CONDITIONAL_CHECK_FAILED;

/**
 * Alibaba implementation of DocStore with table-store
 */
@AutoService(AbstractDocStore.class)
public class AliDocStore extends AbstractDocStore {
    private SyncClient tableStoreClient;
    private IAcsClient hbrClient;
    private final int batchSize = 50;
    DescribeTableResponse tableDescription;

    public AliDocStore(Builder builder) {
        super(builder);
        this.tableStoreClient = builder.tableStoreClient;
        this.hbrClient = builder.hbrClient;
    }

    public AliDocStore() {
        super(new Builder());
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
        } else if (exceptions.stream().anyMatch(TableStoreException.class::isInstance)) {
            String errorCode = ((TableStoreException) t).getErrorCode();
            return ErrorCodeMapping.getException(errorCode);
        } else if (exceptions.stream().anyMatch(ClientException.class::isInstance) ||
                exceptions.stream().anyMatch(IllegalArgumentException.class::isInstance)) {
            return InvalidArgumentException.class;
        }
        return UnknownException.class;
    }

    public static class Builder extends AbstractDocStore.Builder<AliDocStore, Builder> {
        private SyncClient tableStoreClient;
        private IAcsClient hbrClient;

        private static SyncClient buildSyncClient(Builder builder) {
            String endpoint = getTableStoreEndpoint(builder.getRegion(), builder.getInstanceId(), builder.getEndpointType());

            CredentialsProvider provider = TableStoreCredentialsProvider.getCredentialsProvider(builder.getCredentialsOverrider(), builder.getRegion());
            return new SyncClient(endpoint, provider, builder.getInstanceId(), null, new ResourceManager(null, null));
        }

        private static IAcsClient buildHbrClient(Builder builder) {
            com.salesforce.multicloudj.sts.model.CredentialsOverrider credOverrider = builder.getCredentialsOverrider();
            String accessKeyId = null;
            String accessKeySecret = null;

            if (credOverrider != null && credOverrider.getSessionCredentials() != null) {
                accessKeyId = credOverrider.getSessionCredentials().getAccessKeyId();
                accessKeySecret = credOverrider.getSessionCredentials().getAccessKeySecret();
            } else {
                // Fallback to environment variables
                accessKeyId = System.getenv("TABLESTORE_ACCESS_KEY_ID");
                accessKeySecret = System.getenv("TABLESTORE_ACCESS_KEY_SECRET");
            }

            DefaultProfile profile = DefaultProfile.getProfile(builder.getRegion(), accessKeyId, accessKeySecret);
            return new DefaultAcsClient(profile);
        }

        public Builder() {
            providerId("ali");
        }

        @Override
        public Builder self() {
            return this;
        }

        public Builder withTableStoreClient(SyncClient tableStoreClient) {
            this.tableStoreClient = tableStoreClient;
            return this;
        }

        public Builder withHbrClient(IAcsClient hbrClient) {
            this.hbrClient = hbrClient;
            return this;
        }

        @Override
        public AliDocStore build() {
            if (tableStoreClient == null) {
                tableStoreClient = buildSyncClient(this);
            }
            if (hbrClient == null) {
                hbrClient = buildHbrClient(this);
            }
            return new AliDocStore(this);
        }
    }

    private static String getTableStoreEndpoint(String regionId, String instanceId, String endpointType) {
        if (endpointType == null) {
            return String.format("https://%s.%s.vpc.tablestore.aliyuncs.com", instanceId, regionId);
        }
        switch (endpointType) {
            case "internet":
                return String.format("https://%s.%s.ots.aliyuncs.com", instanceId, regionId);
            case "dualstack":
                return String.format("https://%s.%s.tablestore.aliyuncs.com", instanceId, regionId);
            case "intranet":
                return String.format("https://%s.%s.ots-internal.aliyuncs.com", instanceId, regionId);
            default:
                return String.format("https://%s.%s.vpc.tablestore.aliyuncs.com", instanceId, regionId);
        }
    }

    @Override
    public Object getKey(Document document) {
        String partitionKey = (String) document.getField(collectionOptions.getPartitionKey());
        if (partitionKey == null || partitionKey.isBlank()) {
            throw new IllegalArgumentException("Partition key cannot be null or empty");
        }

        String sortKey = collectionOptions.getSortKey() != null
                ? (String) document.getField(collectionOptions.getSortKey())
                : "";

        return String.format("partitionKey:%s,sortKey:%s", partitionKey, sortKey);
    }

    public void runActions(List<Action> actions, Consumer<Predicate<Object>> beforeDo) {
        List<Action> preActions = new ArrayList<>();
        List<Action> writeActions = new ArrayList<>();
        List<Action> atomicWriteActions = new ArrayList<>();
        List<Action> readActions = new ArrayList<>();
        List<Action> postActions = new ArrayList<>();

        Util.groupActions(actions, preActions, readActions, writeActions, atomicWriteActions, postActions);
        try {
            // Run preliminary get actions
            runGets(preActions, beforeDo, batchSize);

            // Run write actions asynchronously while proceeding with read actions in parallel
            CompletableFuture<Void> writeTask = CompletableFuture.runAsync(() -> runWrites(writeActions, beforeDo));
            CompletableFuture<Void> txWriteTask = CompletableFuture.runAsync(() -> runTxWrites(atomicWriteActions, beforeDo));

            runGets(readActions, beforeDo, batchSize);

            // Await completion of write actions
            writeTask.get();
            txWriteTask.get();

            // Run post-action gets
            runGets(postActions, beforeDo, batchSize);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SubstrateSdkException("Interrupted during running actions.", e);
        } catch (ExecutionException e) {
            if (e.getCause() == null) {
                throw new SubstrateSdkException(e);
            }
            throw (RuntimeException) e.getCause();
        }
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

    private String missingKeyField(Map<String, ColumnValue> m) {
        if (m.get(collectionOptions.getPartitionKey()) == null) {
            return collectionOptions.getPartitionKey();
        } else if (collectionOptions.getSortKey() != null && m.get(collectionOptions.getSortKey()) == null) {
            return collectionOptions.getSortKey();
        } else {
            return "";
        }
    }

    protected WriteOperation newUpdate(Action action, Consumer<Predicate<Object>> beforeDo) {
        return null;
    }

    protected WriteOperation newDelete(Action action, Consumer<Predicate<Object>> beforeDo) {
        Map<String, ColumnValue> kv = AliCodec.encodeDoc(action.getDocument());
        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();

        pkBuilder.addPrimaryKeyColumn(collectionOptions.getPartitionKey(), PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getPartitionKey())));
        if (collectionOptions.getSortKey() != null) {
            pkBuilder.addPrimaryKeyColumn(collectionOptions.getSortKey(), PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getSortKey())));
        }


        PrimaryKey primaryKey = pkBuilder.build();
        RowDeleteChange rowChange = new RowDeleteChange(collectionOptions.getTableName(), primaryKey);
        return new WriteOperation(action,
                new PutRowRequest(),
                null,
                null,
                () -> runDelete(rowChange, action, beforeDo)
        );
    }

    protected WriteOperation newPut(Action action, Consumer<Predicate<Object>> beforeDo) {
        Map<String, ColumnValue> kv = AliCodec.encodeDoc(action.getDocument());

        String mf = missingKeyField(kv);
        if (action.getKind() != ActionKind.ACTION_KIND_CREATE && !mf.isEmpty()) {
            throw new IllegalArgumentException("Missing key field: " + mf);
        }

        String newPartitionKey = Util.uniqueString();
        if (collectionOptions.getSortKey() != null && mf.equals(collectionOptions.getSortKey())) {
            throw new IllegalArgumentException("Missing soft key: " + mf);
        }

        PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
        if (mf.equals(collectionOptions.getPartitionKey())) {
            pkBuilder.addPrimaryKeyColumn(collectionOptions.getPartitionKey(), PrimaryKeyValue.fromString(newPartitionKey));
        } else {
            pkBuilder.addPrimaryKeyColumn(collectionOptions.getPartitionKey(), PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getPartitionKey())));
            if (collectionOptions.getSortKey() != null) {
                pkBuilder.addPrimaryKeyColumn(collectionOptions.getSortKey(), PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getSortKey())));
            }
        }

        PrimaryKey primaryKey = pkBuilder.build();
        RowPutChange rowChange = new RowPutChange(collectionOptions.getTableName(), primaryKey);
        buildPreCondition(action, rowChange);

        String rev = null;
        if (action.getDocument().hasField(getRevisionField())) {
            rev = Util.uniqueString();
            rowChange.addColumn(getRevisionField(), AliCodec.encodeValue(rev));
        }

        for (Map.Entry<String, ColumnValue> entry : kv.entrySet()) {
            if (!entry.getKey().equals(getRevisionField()) && !entry.getKey().equals(collectionOptions.getPartitionKey()) && !entry.getKey().equals(collectionOptions.getSortKey())) {
                rowChange.addColumn(entry.getKey(), entry.getValue());
            }
        }

        return new WriteOperation(action,
                new PutRowRequest(rowChange),
                newPartitionKey,
                rev,
                () -> runPut(rowChange, action, beforeDo)
        );
    }

    private Condition buildRevisionPrecondition(Document doc, String revField) {
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

        Condition condition = new Condition();
        SingleColumnValueCondition singleColumnValueCondition = new SingleColumnValueCondition(revField,
                SingleColumnValueCondition.CompareOperator.EQUAL, ColumnValue.fromString(v));
        condition.setColumnCondition(singleColumnValueCondition);
        return condition;
    }

    private void buildPreCondition(Action a, RowPutChange rowPutChange) {
        switch (a.getKind()) {
            case ACTION_KIND_CREATE:
                rowPutChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
                return;
            case ACTION_KIND_UPDATE:
            case ACTION_KIND_REPLACE:
                Condition condition = buildRevisionPrecondition(a.getDocument(), getRevisionField());
                rowPutChange.setCondition(Objects.requireNonNullElseGet(condition, () -> new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST)));
                return;
            case ACTION_KIND_DELETE:
            case ACTION_KIND_PUT:
                // Precondition: the revision matches, if any.
                rowPutChange.setCondition(buildRevisionPrecondition(a.getDocument(), getRevisionField()));
                return;
            case ACTION_KIND_GET:
                // No preconditions on a Get.
                return;
            default:
                throw new IllegalArgumentException("Invalid action kind: " + a.getKind());
        }
    }

    protected void runPut(RowPutChange put, Action action, Consumer<Predicate<Object>> beforeDo) {
        PutRowRequest putRowRequest = new PutRowRequest(put);
        try {
            tableStoreClient.putRow(putRowRequest);
        } catch (TableStoreException exception) {
            if (exception.getErrorCode() != null && exception.getErrorCode().equals(OTS_CONDITIONAL_CHECK_FAILED)) {
                if (action.getKind() == ActionKind.ACTION_KIND_CREATE) {
                    throw new ResourceAlreadyExistsException(exception);
                } else {
                    throw new ResourceNotFoundException(exception);
                }
            }
        }
    }

    protected void runDelete(RowDeleteChange delete, Action action, Consumer<Predicate<Object>> beforeDo) {
        DeleteRowRequest deleteRowRequest = new DeleteRowRequest(delete);
        tableStoreClient.deleteRow(deleteRowRequest);
    }

    protected void batchGet(List<Action> actions, Consumer<Predicate<Object>> beforeDo, int start, int end) {
        // Validate inputs
        if (actions == null || actions.isEmpty() || start < 0 || end >= actions.size()) {
            throw new IllegalArgumentException("Invalid range or empty actions list.");
        }

        MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(collectionOptions.getTableName());
        criteria.setMaxVersions(1);

        // Collect primary keys for criteria
        for (int i = start; i <= end; i++) {
            PrimaryKey pk = AliCodec.encodeDocKeyFields(
                    actions.get(i).getDocument(),
                    collectionOptions.getPartitionKey(),
                    collectionOptions.getSortKey()
            );
            if (pk == null) {
                throw new IllegalArgumentException("Failed to encode keys for action at index: " + i);
            }
            criteria.addRow(pk);
        }

        // Set field paths, ensuring key fields are included
        List<String> fieldPaths = actions.get(start).getFieldPaths();
        if (fieldPaths != null && !fieldPaths.isEmpty()) {
            addColumnsToCriteria(fieldPaths, criteria);

            if (beforeDo != null) {
                beforeDo.accept(object -> true);
            }
        }

        // Execute batch get and process responses
        BatchGetRowRequest batchGetItemRequest = new BatchGetRowRequest();
        batchGetItemRequest.addMultiRowQueryCriteria(criteria);

        BatchGetRowResponse batchGetItemResponse = tableStoreClient.batchGetRow(batchGetItemRequest);
        List<BatchGetRowResponse.RowResult> responses = batchGetItemResponse.getBatchGetRowResult(collectionOptions.getTableName());

        // Map actions by key for quick lookup
        Map<String, Action> actionMap = actions.subList(start, end + 1).stream()
                .collect(Collectors.toMap(action -> action.getKey().toString(), action -> action));

        // Decode each row and update corresponding action documents
        for (BatchGetRowResponse.RowResult item : responses) {
            // the batch get row response is for table -> rows mapping.
            // if table have no rows returned, the list be there with one element
            // but with null rows.
            if (item.getRow() == null) {
                continue;
            }
            Document keyOnlyDoc = createKeyOnlyDocument();
            decodeDoc(item.getRow(), keyOnlyDoc);

            Object decodedKey = getKey(keyOnlyDoc);
            Action action = actionMap.get(decodedKey.toString());
            if (action != null) {
                decodeDoc(item.getRow(), action.getDocument());
            }
        }
    }

    // Helper method to add field paths to criteria
    private void addColumnsToCriteria(List<String> fieldPaths, MultiRowQueryCriteria criteria) {
        boolean hasPartitionKey = false;
        boolean hasSortKey = collectionOptions.getSortKey() == null;

        for (String field : fieldPaths) {
            criteria.addColumnsToGet(field);
            if (field.equals(collectionOptions.getPartitionKey())) {
                hasPartitionKey = true;
            }
            if (field.equals(collectionOptions.getSortKey())) {
                hasSortKey = true;
            }
        }

        if (!hasPartitionKey) {
            criteria.addColumnsToGet(collectionOptions.getPartitionKey());
        }
        if (!hasSortKey) {
            criteria.addColumnsToGet(collectionOptions.getSortKey());
        }
    }

    // Helper method to create a key-only document with null values for partition and sort keys
    private Document createKeyOnlyDocument() {
        Map<String, String> keyMap = new HashMap<>();
        keyMap.put(collectionOptions.getPartitionKey(), null);
        keyMap.put(collectionOptions.getSortKey(), null);
        return new Document(keyMap);
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

        List<WriteOperation> operations = new ArrayList<>();
        // Extract the partition key from any of the action which is supposed to be same.
        // If the partition key is not same in all writes, the transaction is anyway going to fail.
        PrimaryKey transactionPK = new PrimaryKey(Collections.singletonList(
                new PrimaryKeyColumn(collectionOptions.getPartitionKey(),
                        PrimaryKeyValue.fromColumn(AliCodec.encodeDoc(writes.get(0).getDocument()).get(collectionOptions.getPartitionKey())))
        ));
        StartLocalTransactionRequest startTransactionRequest = new StartLocalTransactionRequest(collectionOptions.getTableName(), transactionPK);
        StartLocalTransactionResponse startTransactionResponse = tableStoreClient.startLocalTransaction(startTransactionRequest);
        final String transactionId = startTransactionResponse.getTransactionID();

        for (Action w : writes) {
            WriteOperation op = newWriteOperation(w, beforeDo);
            if (op != null) {
                operations.add(op);
                PutRowRequest putRowRequest = op.getPutRowRequest();
                putRowRequest.setTransactionId(transactionId);
                tableStoreClient.putRow(putRowRequest);
            }
        }

        tableStoreClient.commitTransaction(new CommitTransactionRequest(transactionId));
        updateRevision(operations);
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

    @Override
    public DocumentIterator runGetQuery(Query query) {
        QueryRunner qr = planQuery(query);
        if (qr == null) {
            throw new SubstrateSdkException("Failed to get a query runner.");
        }
        AliDocumentIterator iter = new AliDocumentIterator(qr, query.getOffset(), query.getLimit());
        iter.run(AliDocumentIterator.INIT_TOKEN);
        return iter;
    }

    @NoArgsConstructor
    @AllArgsConstructor
    @Getter
    @Setter
    private static class Key {
        private String partitionKey;
        private String sortKey;

        @Override
        public String toString() {
            return "partitionKey:" + partitionKey + "," + "sortKey:" + sortKey;
        }
    }

    @AllArgsConstructor
    @Getter
    private static class Queryable {
        private String indexName;
        private Key key;
    }

    @Override
    public String queryPlan(Query query) {
        QueryRunner qr = planQuery(query);
        return qr.queryPlan();
    }

    public QueryRunner planQuery(Query query) {
        Queryable queryable = getBestQueryable(query);
        boolean isScan = false;
        if (queryable.indexName == null && queryable.key == null) {
            // No query can be done: fall back to scanning.
            if (query.getOrderByField() != null && !query.getOrderByField().isEmpty()) {
                throw new InvalidArgumentException("query requires a table scan, but has an ordering requirement; add an index or provide Options.RunQueryFallback");
            }

            isScan = true;
        }

        // Build the SQL statement
        String sqlStatement = buildSQLStatement(query, queryable);

        // Execute the SQL query
        SQLQueryRequest sqlQueryRequest = new SQLQueryRequest(sqlStatement);
        return new QueryRunner(tableStoreClient, sqlQueryRequest, isScan, query.getBeforeQuery());
    }

    // Reports whether query has a filter that checks if the top-level field is equal to something.
    protected boolean hasEqualityFilter(Query query, String field) {
        if (StringUtils.isEmpty(field)) {
            return false;
        }
        for (Filter filter : query.getFilters()) {
            if (filter.getOp() == FilterOperation.EQUAL && filter.getFieldPath().equals(field)) {
                return true;
            }
        }
        return false;
    }

    // Reports whether query has a filter that mentions the top-level field.
    protected boolean hasFilter(Query query, String field) {
        if (StringUtils.isEmpty(field)) {
            return false;
        }
        for (Filter filter : query.getFilters()) {
            if (filter.getFieldPath().equals(field)) {
                return true;
            }
        }
        return false;
    }

    // orderingConsistent reports whether the ordering constraint is consistent with the sort key field.
    // That is, either there is no OrderBy clause, or the clause specifies the sort field.
    private boolean orderingConsistent(Query query, String sortedField) {
        return StringUtils.isEmpty(query.getOrderByField()) || query.getOrderByField().equals(sortedField);
    }

    protected boolean globalFieldIncluded(Query query, IndexMeta gi) {
        if (query.getFieldPaths().isEmpty()) {
            // The query wants all the fields of the table
            return false;
        }

        Key key = keyAttributes(gi.getPrimaryKeyList());
        Map<String, Boolean> indexFields = new HashMap<>();
        indexFields.put(key.getPartitionKey(), true);
        if (key.getSortKey() != null) {
            indexFields.put(key.getSortKey(), true);
        }
        for (String nka : gi.getDefinedColumnsList()) {
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

    protected DescribeTableResponse getTableDescription() {
        if (tableDescription == null) {
            DescribeTableRequest describeTableRequest = new DescribeTableRequest(collectionOptions.getTableName());
            tableDescription = tableStoreClient.describeTable(describeTableRequest);
        }

        return tableDescription;
    }

    // Extract the names of the partition and sort key attributes from the schema of a
    // table or index.
    protected Key keyAttributes(List<String> keySchemaElements) {
        if (keySchemaElements.isEmpty()) {
            throw new IllegalArgumentException("Partition key should be always there");
        }
        Key key = new Key();
        key.setPartitionKey(keySchemaElements.get(0));
        if (keySchemaElements.size() > 1) {
            key.setSortKey(keySchemaElements.get(1));
        }
        return key.getPartitionKey() != null ? key : null;
    }

    private boolean isValidSortKey(Query query, String sortKey) {
        return hasFilter(query, sortKey) && orderingConsistent(query, sortKey);
    }

    protected Queryable getBestQueryable(Query query) {
        Queryable result = getMatchingBaseTableOrLocalIndexes(query);
        if (result != null) return result;

        result = getMatchingGlobalIndexesWithSortKey(query);
        if (result != null) return result;

        result = getMatchingBaseTableForPartitionOnly(query);
        if (result != null) return result;

        result = getMatchingGlobalIndexesForPartitionOnly(query);
        if (result != null) return result;

        return new Queryable(null, null);
    }

    private Queryable getMatchingBaseTableOrLocalIndexes(Query query) {
        // local index have the same partition key as base table, there is no point
        // moving forward if equality check is no there on partition key.
        if (!hasEqualityFilter(query, collectionOptions.getPartitionKey())) {
            return null;
        }

        // If the table has a sort key that's in the query, and the ordering
        // constraints works with the sort key, use the table.
        // (Query results are always ordered by the sort key.)
        if (isValidSortKey(query, collectionOptions.getSortKey())) {
            return createBaseTableQueryable();
        }

        return getMatchingLocalIndexes(query);
    }

    private Queryable createBaseTableQueryable() {
        return new Queryable(null, new Key(collectionOptions.getPartitionKey(), collectionOptions.getSortKey()));
    }

    // Using local index is good if sort key matches with that of the local index
    private Queryable getMatchingLocalIndexes(Query query) {
        for (IndexMeta index : getTableDescription().getIndexMeta()) {
            if (index.getIndexType() != IndexType.IT_LOCAL_INDEX) continue;

            Key key = getKeyForIndex(index);
            if (isLocalIndexSuitable(query, key, index)) {
                return new Queryable(index.getIndexName(), key);
            }
        }
        return null;
    }

    private Key getKeyForIndex(IndexMeta index) {
        return keyAttributes(index.getPrimaryKeyList());
    }

    // Validate if local index have all the fields from the query and sort key matches
    // with the order by clause.
    private boolean isLocalIndexSuitable(Query query, Key key, IndexMeta index) {
        Set<String> fields = new HashSet<>();
        fields.addAll(index.getDefinedColumnsList());
        fields.addAll(index.getPrimaryKeyList());
        return key != null && fields.containsAll(query.getFieldPaths()) && isValidSortKey(query, key.getSortKey());
    }

    private Queryable getMatchingGlobalIndexesWithSortKey(Query query) {
        for (IndexMeta index : getTableDescription().getIndexMeta()) {
            if (index.getIndexType() != IndexType.IT_GLOBAL_INDEX) continue;

            Key key = getKeyForIndex(index);
            if (isGlobalIndexWithSortKeySuitable(query, index, key)) {
                return new Queryable(index.getIndexName(), key);
            }
        }
        return null;
    }

    // Global indexes should contain all the fields in the query, the sort should
    // match with the order by clause in the query
    private boolean isGlobalIndexWithSortKeySuitable(Query query, IndexMeta index, Key key) {
        return key != null
                && key.getSortKey() != null
                && hasEqualityFilter(query, key.getPartitionKey())
                && globalFieldIncluded(query, index)
                && isValidSortKey(query, key.getSortKey());
    }

    private Queryable getMatchingBaseTableForPartitionOnly(Query query) {
        return hasEqualityFilter(query, collectionOptions.getPartitionKey())
                ? createBaseTableQueryable()
                : null;
    }

    private Queryable getMatchingGlobalIndexesForPartitionOnly(Query query) {
        for (IndexMeta index : getTableDescription().getIndexMeta()) {
            if (index.getIndexType() != IndexType.IT_GLOBAL_INDEX) continue;

            Key key = getKeyForIndex(index);
            if (isGlobalIndexPartitionOnlySuitable(query, index, key)) {
                return new Queryable(index.getIndexName(), key);
            }
        }
        return null;
    }

    private boolean isGlobalIndexPartitionOnlySuitable(Query query, IndexMeta index, Key key) {
        return key != null
                && hasEqualityFilter(query, key.getPartitionKey())
                && globalFieldIncluded(query, index);
    }

    private String buildSQLStatement(Query query, Queryable queryable) {
        StringBuilder sql = new StringBuilder();
        List<String> fields = query.getFieldPaths();
        if (fields == null || fields.isEmpty()) {
            fields = List.of("*");
        }

        sql.append("SELECT ").append(String.join(",", fields));
        if (queryable.indexName != null) {
            sql.append(" FROM ").append(queryable.indexName);
        } else {
            sql.append(" FROM ").append(collectionOptions.getTableName());
        }
        if (query.getFilters() != null && !query.getFilters().isEmpty()) {
            sql.append(" WHERE ").append(conditionBuilder(query.getFilters()));
        }

        if (query.getOrderByField() != null) {
            sql.append(" ORDER BY ").append(query.getOrderByField());
            // the default ORDER is ASC we don't need to add it explicitly.
            if (!query.isOrderAscending()) {
                sql.append(" DESC");
            }
        }

        if (query.getLimit() > 0) {
            sql.append(" LIMIT ").append(query.getLimit());
        }

        if (query.getOffset() > 0) {
            sql.append(" OFFSET ").append(query.getOffset());
        }

        sql.append(";");
        return sql.toString();
    }

    private String conditionBuilder(List<Filter> filters) {
        final Map<FilterOperation, String> filterOperationMapping = Map.of(
                FilterOperation.EQUAL, "=",
                FilterOperation.GREATER_THAN, ">",
                FilterOperation.GREATER_THAN_OR_EQUAL_TO, ">=",
                FilterOperation.LESS_THAN, "<",
                FilterOperation.LESS_THAN_OR_EQUAL_TO, "<=",
                FilterOperation.NOT_IN, "NOT IN",
                FilterOperation.IN, "IN"
        );

        StringBuilder condition = new StringBuilder();
        for (int i = 0; i < filters.size(); i++) {
            Filter filter = filters.get(i);
            if (i > 0) {
                condition.append(" AND ");
            }
            condition.append(filter.getFieldPath()).append(" ")
                    .append(filterOperationMapping.get(filter.getOp())).append(" ")
                    .append("'").append(filter.getValue()).append("'");
        }
        return condition.toString();
    }

    // Close cleans up any resources used by the Collection.
    @Override
    public void close() {
        this.executorService.shutdown();
        tableStoreClient.shutdown();
    }

    @Override
    public java.util.List<com.salesforce.multicloudj.docstore.driver.Backup> listBackups() {
        throw new com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException(
                "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. " +
                "Listing backups requires HBR vault configuration. " +
                "Use Alibaba Cloud Console to view backups or configure HBR programmatically.");
    }

    @Override
    public com.salesforce.multicloudj.docstore.driver.Backup getBackup(String backupId) {
        throw new com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException(
                "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. " +
                "Getting backup details requires HBR vault configuration. " +
                "Use Alibaba Cloud Console or configure HBR programmatically.");
    }

    @Override
    public com.salesforce.multicloudj.docstore.driver.BackupStatus getBackupStatus(String backupId) {
        throw new com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException(
                "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. " +
                "Getting backup status requires HBR vault configuration. " +
                "Use Alibaba Cloud Console or configure HBR programmatically.");
    }

    @Override
    public void restoreBackup(com.salesforce.multicloudj.docstore.driver.RestoreRequest request) {
        if (hbrClient == null) {
            throw new SubstrateSdkException("HBR client not initialized. " +
                    "Ensure credentials are configured in the builder.");
        }

        try {
            CreateRestoreJobRequest restoreRequest = new CreateRestoreJobRequest();
            restoreRequest.setRestoreType("OTS_TABLE");
            restoreRequest.setSnapshotId(request.getBackupId());

            // Set the target table name
            String targetTableName = request.getTargetCollectionName() != null
                    && !request.getTargetCollectionName().isEmpty()
                    ? request.getTargetCollectionName()
                    : collectionOptions.getTableName() + "-restored";

            // Note: Alibaba HBR restore for TableStore requires additional configuration
            // such as vault ID and instance name. These would need to be provided
            // through the RestoreRequest or collection options.
            restoreRequest.setSourceType("OTS_TABLE");

            CreateRestoreJobResponse response = hbrClient.getAcsResponse(restoreRequest);

            if (!response.getSuccess()) {
                throw new SubstrateSdkException("Failed to create restore job: " +
                        response.getMessage() + " (Code: " + response.getCode() + ")");
            }
        } catch (Exception e) {
            if (e instanceof SubstrateSdkException) {
                throw (SubstrateSdkException) e;
            }
            throw new SubstrateSdkException("Failed to restore Alibaba TableStore backup", e);
        }
    }

    @Override
    public void deleteBackup(String backupId) {
        throw new com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException(
                "Alibaba TableStore backups are managed through HBR (Hybrid Backup Recovery) service. " +
                "Deleting backups requires HBR vault configuration. " +
                "Use Alibaba Cloud Console or configure HBR programmatically.");
    }

}