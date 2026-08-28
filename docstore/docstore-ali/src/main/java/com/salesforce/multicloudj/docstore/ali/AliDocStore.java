package com.salesforce.multicloudj.docstore.ali;

import static com.salesforce.multicloudj.docstore.ali.AliCodec.decodeDoc;
import static com.salesforce.multicloudj.docstore.ali.ErrorCodeMapping.OTS_CONDITIONAL_CHECK_FAILED;

import com.alicloud.openservices.tablestore.ClientException;
import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TableStoreException;
import com.alicloud.openservices.tablestore.core.ResourceManager;
import com.alicloud.openservices.tablestore.core.auth.CredentialsProvider;
import com.alicloud.openservices.tablestore.model.AbortTransactionRequest;
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
import com.alicloud.openservices.tablestore.model.RowChange;
import com.alicloud.openservices.tablestore.model.RowDeleteChange;
import com.alicloud.openservices.tablestore.model.RowExistenceExpectation;
import com.alicloud.openservices.tablestore.model.RowPutChange;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionRequest;
import com.alicloud.openservices.tablestore.model.StartLocalTransactionResponse;
import com.alicloud.openservices.tablestore.model.condition.SingleColumnValueCondition;
import com.google.auto.service.AutoService;
import com.salesforce.multicloudj.common.ali.AliRetryClassifier;
import com.salesforce.multicloudj.common.exceptions.ExceptionHandler;
import com.salesforce.multicloudj.common.exceptions.InvalidArgumentException;
import com.salesforce.multicloudj.common.exceptions.ResourceAlreadyExistsException;
import com.salesforce.multicloudj.common.exceptions.ResourceNotFoundException;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import com.salesforce.multicloudj.common.exceptions.TransactionFailedException;
import com.salesforce.multicloudj.common.exceptions.UnSupportedOperationException;
import com.salesforce.multicloudj.common.exceptions.UnknownException;
import com.salesforce.multicloudj.common.util.UUID;
import com.salesforce.multicloudj.docstore.client.Query;
import com.salesforce.multicloudj.docstore.driver.AbstractDocStore;
import com.salesforce.multicloudj.docstore.driver.Action;
import com.salesforce.multicloudj.docstore.driver.ActionKind;
import com.salesforce.multicloudj.docstore.driver.Document;
import com.salesforce.multicloudj.docstore.driver.DocumentIterator;
import com.salesforce.multicloudj.docstore.driver.Filter;
import com.salesforce.multicloudj.docstore.driver.FilterOperation;
import com.salesforce.multicloudj.docstore.driver.Util;
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
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/** Alibaba implementation of DocStore with table-store */
@AutoService(AbstractDocStore.class)
public class AliDocStore extends AbstractDocStore {
  private SyncClient tableStoreClient;
  private final int batchSize = 50;

  // Tablestore caps a single GetRange page at 5000 rows / 4 MB regardless of any client-set limit.
  private static final int MAX_GETRANGE_ROWS = 5000;

  DescribeTableResponse tableDescription;

  public AliDocStore(Builder builder) {
    super(builder);
    this.tableStoreClient = builder.tableStoreClient;
  }

  public AliDocStore() {
    super(new Builder());
  }

  @Override
  public Builder builder() {
    return new Builder();
  }

  @Override
  public SubstrateSdkException mapException(Throwable t) {
    List<Throwable> causeChain =
        ExceptionUtils.getThrowableList(t).stream().limit(5).collect(Collectors.toList());
    Class<? extends SubstrateSdkException> exceptionClass;
    Boolean retryableHint = null;
    TableStoreException tableStoreException =
        causeChain.stream()
            .filter(TableStoreException.class::isInstance)
            .map(TableStoreException.class::cast)
            .findFirst()
            .orElse(null);
    if (tableStoreException != null) {
      exceptionClass = ErrorCodeMapping.getException(tableStoreException.getErrorCode());
      retryableHint = AliRetryClassifier.classifyByStatusCode(tableStoreException.getHttpStatus());
    } else if (causeChain.stream().anyMatch(ClientException.class::isInstance)
        || causeChain.stream().anyMatch(IllegalArgumentException.class::isInstance)) {
      exceptionClass = InvalidArgumentException.class;
    } else {
      exceptionClass = UnknownException.class;
    }
    return ExceptionHandler.build(exceptionClass, t, retryableHint);
  }

  public static class Builder extends AbstractDocStore.Builder<AliDocStore, Builder> {
    private SyncClient tableStoreClient;

    private static SyncClient buildSyncClient(Builder builder) {
      String endpoint =
          getTableStoreEndpoint(
              builder.getRegion(), builder.getInstanceId(), builder.getEndpointType());

      CredentialsProvider provider =
          TableStoreCredentialsProvider.getCredentialsProvider(
              builder.getCredentialsOverrider(), builder.getRegion());
      return new SyncClient(
          endpoint, provider, builder.getInstanceId(), null, new ResourceManager(null, null));
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

    @Override
    public AliDocStore build() {
      if (tableStoreClient == null) {
        tableStoreClient = buildSyncClient(this);
      }
      return new AliDocStore(this);
    }
  }

  private static String getTableStoreEndpoint(
      String regionId, String instanceId, String endpointType) {
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

    String sortKey =
        collectionOptions.getSortKey() != null
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

    Util.groupActions(
        actions, preActions, readActions, writeActions, atomicWriteActions, postActions);
    try {
      // Run preliminary get actions
      runGets(preActions, beforeDo, batchSize);

      CompletableFuture<Void> writeTask =
          CompletableFuture.runAsync(() -> runWrites(writeActions, beforeDo));

      // When the action list contains an atomic transaction, the non-atomic writes must COMPLETE
      // before the transaction starts. A Tablestore local transaction takes an exclusive lock on
      // its partition; if a non-atomic write targets the same partition (e.g. a delete and an
      // atomic put under one partition key) running it concurrently with the transaction races that
      // lock and fails with OTSRowOperationConflict. Reads do not take the write lock, so the
      // transaction still overlaps the read actions below. When there is no atomic transaction, the
      // writes and reads run concurrently as before (the barrier is unnecessary).
      if (!atomicWriteActions.isEmpty()) {
        writeTask.get();
      }

      CompletableFuture<Void> txWriteTask =
          CompletableFuture.runAsync(() -> runTxWrites(atomicWriteActions, beforeDo));

      runGets(readActions, beforeDo, batchSize);

      // Await completion of the write actions and the atomic transaction
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
      case ACTION_KIND_CREATE: // Fall through
      case ACTION_KIND_REPLACE: // Fall through
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
    } else if (collectionOptions.getSortKey() != null
        && m.get(collectionOptions.getSortKey()) == null) {
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

    pkBuilder.addPrimaryKeyColumn(
        collectionOptions.getPartitionKey(),
        PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getPartitionKey())));
    if (collectionOptions.getSortKey() != null) {
      pkBuilder.addPrimaryKeyColumn(
          collectionOptions.getSortKey(),
          PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getSortKey())));
    }

    PrimaryKey primaryKey = pkBuilder.build();
    RowDeleteChange rowChange = new RowDeleteChange(collectionOptions.getTableName(), primaryKey);
    // Enforce the optimistic-concurrency revision precondition on delete: when the document carries
    // a revision, the delete only succeeds if the stored row still carries that revision. No
    // revision on the document means an unconditional delete.
    buildPreCondition(action, rowChange);
    return new WriteOperation(
        action, rowChange, null, null, () -> runDelete(rowChange, action, beforeDo));
  }

  protected WriteOperation newPut(Action action, Consumer<Predicate<Object>> beforeDo) {
    Map<String, ColumnValue> kv = AliCodec.encodeDoc(action.getDocument());

    String mf = missingKeyField(kv);
    if (action.getKind() != ActionKind.ACTION_KIND_CREATE && !mf.isEmpty()) {
      throw new IllegalArgumentException("Missing key field: " + mf);
    }

    String newPartitionKey = UUID.uniqueString();
    if (collectionOptions.getSortKey() != null && mf.equals(collectionOptions.getSortKey())) {
      throw new IllegalArgumentException("Missing soft key: " + mf);
    }

    PrimaryKeyBuilder pkBuilder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    if (mf.equals(collectionOptions.getPartitionKey())) {
      pkBuilder.addPrimaryKeyColumn(
          collectionOptions.getPartitionKey(), PrimaryKeyValue.fromString(newPartitionKey));
    } else {
      pkBuilder.addPrimaryKeyColumn(
          collectionOptions.getPartitionKey(),
          PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getPartitionKey())));
      if (collectionOptions.getSortKey() != null) {
        pkBuilder.addPrimaryKeyColumn(
            collectionOptions.getSortKey(),
            PrimaryKeyValue.fromColumn(kv.get(collectionOptions.getSortKey())));
      }
    }

    PrimaryKey primaryKey = pkBuilder.build();
    RowPutChange rowChange = new RowPutChange(collectionOptions.getTableName(), primaryKey);
    buildPreCondition(action, rowChange);

    String rev = null;
    if (action.getDocument().hasField(getRevisionField())) {
      rev = UUID.uniqueString();
      rowChange.addColumn(getRevisionField(), AliCodec.encodeValue(rev));
    }

    for (Map.Entry<String, ColumnValue> entry : kv.entrySet()) {
      if (!entry.getKey().equals(getRevisionField())
          && !entry.getKey().equals(collectionOptions.getPartitionKey())
          && !entry.getKey().equals(collectionOptions.getSortKey())) {
        rowChange.addColumn(entry.getKey(), entry.getValue());
      }
    }

    return new WriteOperation(
        action,
        rowChange,
        newPartitionKey,
        rev,
        () -> runPut(rowChange, action, beforeDo));
  }

  private Condition buildRevisionPrecondition(Document doc, String revField) {
    Object object = doc.getField(revField);
    if (object == null) {
      return null;
    }
    if (!(object instanceof String)) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid revision field %s type as %s, expect String type.",
              revField, object == null ? null : object.getClass().getName()));
    }
    String v = (String) doc.getField(revField);
    if (v == null || v.isEmpty()) {
      return null;
    }

    Condition condition = new Condition();
    SingleColumnValueCondition singleColumnValueCondition =
        new SingleColumnValueCondition(
            revField, SingleColumnValueCondition.CompareOperator.EQUAL, ColumnValue.fromString(v));
    // Fail the precondition if revision column is not present in the row
    singleColumnValueCondition.setPassIfMissing(false);
    condition.setColumnCondition(singleColumnValueCondition);
    return condition;
  }

  private void buildPreCondition(Action a, RowChange rowChange) {
    switch (a.getKind()) {
      case ACTION_KIND_CREATE:
        rowChange.setCondition(new Condition(RowExistenceExpectation.EXPECT_NOT_EXIST));
        return;
      case ACTION_KIND_UPDATE:
      case ACTION_KIND_REPLACE:
        Condition condition = buildRevisionPrecondition(a.getDocument(), getRevisionField());
        rowChange.setCondition(
            Objects.requireNonNullElseGet(
                condition, () -> new Condition(RowExistenceExpectation.EXPECT_EXIST)));
        return;
      case ACTION_KIND_DELETE:
      case ACTION_KIND_PUT:
        Condition revisionCondition =
            buildRevisionPrecondition(a.getDocument(), getRevisionField());
        if (revisionCondition != null) {
          rowChange.setCondition(revisionCondition);
        }
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
      if (exception.getErrorCode() != null
          && exception.getErrorCode().equals(OTS_CONDITIONAL_CHECK_FAILED)) {
        if (action.getKind() == ActionKind.ACTION_KIND_CREATE) {
          throw new ResourceAlreadyExistsException(exception);
        } else {
          throw new ResourceNotFoundException(exception);
        }
      }
      // Any other Tablestore failure (throttling, quota, server error, invalid request, timeout,
      // etc.) leaves the write unconfirmed. Re-throw and let mapException classify it, rather than
      // report an unconfirmed write as success and stamp a revision for it.
      throw exception;
    }
  }

  protected void runDelete(
      RowDeleteChange delete, Action action, Consumer<Predicate<Object>> beforeDo) {
    DeleteRowRequest deleteRowRequest = new DeleteRowRequest(delete);
    // A delete's only conditional-check failure is a revision mismatch (it sets no existence
    // expectation), so let it propagate and map to FailedPreconditionException.
    tableStoreClient.deleteRow(deleteRowRequest);
  }

  protected void batchGet(
      List<Action> actions, Consumer<Predicate<Object>> beforeDo, int start, int end) {
    // Validate inputs
    if (actions == null || actions.isEmpty() || start < 0 || end >= actions.size()) {
      throw new IllegalArgumentException("Invalid range or empty actions list.");
    }

    MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(collectionOptions.getTableName());
    criteria.setMaxVersions(1);

    // Collect primary keys for criteria
    for (int i = start; i <= end; i++) {
      PrimaryKey pk =
          AliCodec.encodeDocKeyFields(
              actions.get(i).getDocument(),
              collectionOptions.getPartitionKey(),
              collectionOptions.getSortKey());
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
    List<BatchGetRowResponse.RowResult> responses =
        batchGetItemResponse.getBatchGetRowResult(collectionOptions.getTableName());

    // Map actions by key for quick lookup
    Map<String, Action> actionMap =
        actions.subList(start, end + 1).stream()
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
    List<WriteOperation> writeOperations =
        writes.stream()
            .map(action -> newWriteOperation(action, beforeDo))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    // Submit each write operation as a CompletableFuture to the executor
    // Wait for all operations to complete
    CompletableFuture.allOf(
            writeOperations.stream()
                .map(op -> CompletableFuture.runAsync(op.getRun(), executorService))
                .toArray(CompletableFuture[]::new))
        .join();
    updateRevision(writeOperations);
  }

  private void runTxWrites(List<Action> writes, Consumer<Predicate<Object>> beforeDo) {
    if (writes.isEmpty()) {
      return;
    }

    // Build the write operations BEFORE opening the transaction. newWriteOperation validates the
    // document shape and can throw InvalidArgumentException (e.g. a missing key field) — a caller
    // error that must surface as-is, not be remapped to TransactionFailedException. Doing this
    // first also means a malformed request never starts (or leaks) a transaction.
    List<WriteOperation> operations = new ArrayList<>();
    for (Action w : writes) {
      WriteOperation op = newWriteOperation(w, beforeDo);
      if (op != null) {
        operations.add(op);
      }
    }

    // Extract the partition key from any of the action which is supposed to be same.
    // If the partition key is not same in all writes, the transaction is anyway going to fail.
    PrimaryKey transactionPK =
        new PrimaryKey(
            Collections.singletonList(
                new PrimaryKeyColumn(
                    collectionOptions.getPartitionKey(),
                    PrimaryKeyValue.fromColumn(
                        AliCodec.encodeDoc(writes.get(0).getDocument())
                            .get(collectionOptions.getPartitionKey())))));
    StartLocalTransactionRequest startTransactionRequest =
        new StartLocalTransactionRequest(collectionOptions.getTableName(), transactionPK);
    StartLocalTransactionResponse startTransactionResponse =
        tableStoreClient.startLocalTransaction(startTransactionRequest);
    final String transactionId = startTransactionResponse.getTransactionID();

    try {
      for (WriteOperation op : operations) {
        // Tablestore has no single generic "apply this RowChange" call, so dispatch each op to the
        // request type its change requires: deletes go through deleteRow, puts through putRow.
        // Reject any other unsupported RowChange subtype with a clear error rather than blindly
        // casting it to a put and failing with an opaque ClassCastException mid-transaction.
        RowChange change = op.getRowChange();
        if (change instanceof RowDeleteChange) {
          DeleteRowRequest deleteRowRequest = new DeleteRowRequest((RowDeleteChange) change);
          deleteRowRequest.setTransactionId(transactionId);
          tableStoreClient.deleteRow(deleteRowRequest);
        } else if (change instanceof RowPutChange) {
          PutRowRequest putRowRequest = new PutRowRequest((RowPutChange) change);
          putRowRequest.setTransactionId(transactionId);
          tableStoreClient.putRow(putRowRequest);
        } else {
          throw new UnSupportedOperationException(
              "Unsupported RowChange type in atomic write: "
                  + (change == null ? "null" : change.getClass().getName()));
        }
      }
      tableStoreClient.commitTransaction(new CommitTransactionRequest(transactionId));
    } catch (RuntimeException e) {
      // Any failure of the transactional TableStore calls — a rejected write/commit
      // (TableStoreException, e.g. OTSConditionCheckFail on a non-existent row) or a transport
      // failure (ClientException, e.g. a network timeout), both RuntimeExceptions — must fail the
      // whole block: atomic writes are all-or-nothing. Abort so the transaction is not left
      // dangling, then surface a uniform TransactionFailedException regardless of the underlying
      // cause. (Document-shape validation happens above, before the transaction opens, so caller
      // errors are not remapped here.)
      TransactionFailedException failure =
          new TransactionFailedException("Atomic write failed - all operations rolled back", e);
      try {
        tableStoreClient.abortTransaction(new AbortTransactionRequest(transactionId));
      } catch (RuntimeException abortFailure) {
        // Best-effort rollback. If the abort itself fails, the server-side transaction and its
        // exclusive partition lock dangle until Tablestore's TTL; attach the abort failure as a
        // suppressed exception so the diagnostic is not lost, then surface the original cause.
        failure.addSuppressed(abortFailure);
      }
      throw failure;
    }
    updateRevision(operations);
  }

  private void updateRevision(List<WriteOperation> writeOperations) {
    writeOperations.forEach(
        writeOperation -> {
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
    Queryable queryable = getBestQueryable(query);
    checkPlan(query, queryable);

    QueryRunner runner = planGetRangeQuery(query, queryable);

    PrimaryKey resumeAfterKey = null;
    if (query.getPaginationToken() instanceof AliPaginationToken) {
      resumeAfterKey = ((AliPaginationToken) query.getPaginationToken()).getNextStartPrimaryKey();
    }
    return new AliDocumentIterator(runner, query.getOffset(), query.getLimit(), resumeAfterKey);
  }

  // Rejects query plans that cannot be served: a full-table scan when ordering is requested (a scan
  // yields primary-key order only), or a scan when Options.AllowScans is disabled.
  private void checkPlan(Query query, Queryable queryable) {
    boolean isScan = queryable.getIndexName() == null && queryable.getKey() == null;
    if (!isScan) {
      return;
    }
    if (StringUtils.isNotEmpty(query.getOrderByField())) {
      throw new InvalidArgumentException(
          "query requires a table scan, but has an ordering requirement; add a secondary index"
              + " whose key matches the order-by field");
    }
    if (!collectionOptions.isAllowScans()) {
      throw new InvalidArgumentException(
          "query requires a table scan; set Options.AllowScans to true to enable");
    }
  }

  // Translates the query into a GetRange runner over the resolved base table or secondary index.
  private QueryRunner planGetRangeQuery(Query query, Queryable queryable) {
    List<String> pkColumns = getPrimaryKeyColumns(queryable);
    String targetTable =
        queryable.getIndexName() != null
            ? queryable.getIndexName()
            : collectionOptions.getTableName();

    List<Filter> filters = query.getFilters() != null ? query.getFilters() : List.of();
    QueryPlanner.Plan plan =
        QueryPlanner.plan(pkColumns, filters, query.isOrderAscending());

    // An unprojected query served from a secondary index must hydrate each row from the base table:
    // the index physically carries only its own columns, so it cannot return a row's schema-less
    // attributes. A projected index query and any base-table/scan query never hydrate. See
    // QueryRunner for why hydration is needed and its global-index eventual-consistency behavior.
    boolean hydrateFromBase =
        queryable.getIndexName() != null && ObjectUtils.isEmpty(query.getFieldPaths());
    List<String> baseKeyColumns = getPrimaryKeyColumns(createBaseTableQueryable());

    // Apply the fixed per-page GetRange row cap only when the key range is tight (the range
    // captures the whole predicate set, so the column filter drops at most an O(1) boundary group).
    // When it is NOT tight, the column filter drops a non-trivial number of scanned rows, so a
    // small per-page cap would make each capped page yield few (or zero) matches and force the
    // iterator to re-scan page after page; leave the request unbounded (0) so Tablestore's natural
    // 5000-row/4 MB page does the server-side filtering in one round trip. See QueryPlanner.
    int perRequestLimit =
        plan.isKeyRangeTight() ? computePerRequestLimit(query.getOffset(), query.getLimit()) : 0;

    return new QueryRunner(
        tableStoreClient,
        targetTable,
        plan.getInclusiveStartPrimaryKey(),
        plan.getExclusiveEndPrimaryKey(),
        plan.getDirection(),
        plan.getColumnFilter(),
        buildColumnsToGet(query.getFieldPaths(), pkColumns, filters),
        buildVisibleColumns(query.getFieldPaths(), pkColumns),
        hydrateFromBase ? collectionOptions.getTableName() : null,
        baseKeyColumns,
        batchSize,
        perRequestLimit,
        this::mapException);
  }

  // Per-request GetRange row cap = offset + limit. Applied by planGetRangeQuery ONLY when the
  // plan's key range is tight (see QueryPlanner#computeKeyRangeTight): a tight range captures the
  // whole predicate set, so the column filter drops at most an O(1) boundary group and a capped
  // page still yields ~offset+limit matches. When the range is NOT tight the caller passes 0
  // (unbounded) instead, because the column filter would drop most of what each small capped page
  // scans. This is a FIXED per-page cap applied to EVERY GetRange page, not a running budget: it
  // does not track the iterator's remaining demand across pages, so a multi-page query (a
  // server-side filter dropping rows within a page, or an offset larger than one page) may
  // re-request up to offset + limit rows on each subsequent page. Capping each page still stops a
  // small-limit unprojected index query from scanning and hydrating a full ~5000-row page just to
  // yield a few rows. Returns 0 ("unbounded": leave Tablestore's natural
  // page cap) when the caller set no limit, or when offset+limit meets or exceeds that cap (a limit
  // there would not tighten anything). Uses long arithmetic so a pathological offset/limit cannot
  // overflow int into a negative that setLimit would reject; on overflow it falls through to
  // unbounded (today's behavior). Tightening the later-page cap to the iterator's remaining demand
  // is a possible future optimization.
  static int computePerRequestLimit(int offset, int limit) {
    if (limit <= 0) {
      return 0;
    }
    long budget = (long) Math.max(offset, 0) + (long) limit;
    return budget < MAX_GETRANGE_ROWS ? (int) budget : 0;
  }

  // Builds the columns_to_get list to FETCH for a projected query: the projection, plus the
  // target's primary-key columns, plus every predicate field. An empty field-path list means "all
  // columns", which Tablestore expresses as an empty columns_to_get, so it is returned unchanged.
  //
  // Primary-key columns are force-added if absent: Tablestore's GetRange omits a row from the
  // response when none of its requested columns are present, and both row decoding and the
  // pagination cursor read the primary key, so a missing key would drop matching rows and break
  // continuation. Uses the resolved target's key columns (base-table keys, or a secondary index's
  // own key list) so index queries stay correct.
  //
  // Predicate fields are force-added because Tablestore applies the server-side column filter AFTER
  // columns_to_get: a filter on a field that was not fetched reads as missing and, under
  // passIfMissing(false), drops every matching row. So a projected query with a predicate on a
  // non-projected field must still fetch that field. The extra columns are then trimmed back out of
  // the returned rows (see buildVisibleColumns and QueryRunner) so the caller still sees only the
  // projection plus the primary key.
  private List<String> buildColumnsToGet(
      List<String> fieldPaths, List<String> pkColumns, List<Filter> filters) {
    if (ObjectUtils.isEmpty(fieldPaths)) {
      return fieldPaths;
    }
    List<String> columnsToGet = new ArrayList<>(fieldPaths);
    for (String pkColumn : pkColumns) {
      if (!columnsToGet.contains(pkColumn)) {
        columnsToGet.add(pkColumn);
      }
    }
    for (Filter filter : filters) {
      if (!columnsToGet.contains(filter.getFieldPath())) {
        columnsToGet.add(filter.getFieldPath());
      }
    }
    return columnsToGet;
  }

  // The columns a projected query's caller is allowed to SEE: the projection plus the target's
  // primary-key columns. Returns null when the query is unprojected (empty field paths); null means
  // "all columns visible", so QueryRunner performs no trimming. When a subset is projected,
  // buildColumnsToGet may fetch extra predicate fields the caller did not ask for, and the fetched
  // rows are trimmed back to this set so a non-projected predicate field never leaks to the caller.
  private Set<String> buildVisibleColumns(List<String> fieldPaths, List<String> pkColumns) {
    if (ObjectUtils.isEmpty(fieldPaths)) {
      return null;
    }
    Set<String> visible = new HashSet<>(fieldPaths);
    visible.addAll(pkColumns);
    return visible;
  }

  // Full ordered primary-key column list of the resolved target: the base table's keys from
  // CollectionOptions, or a secondary index's keys from its IndexMeta. A scan (no resolved
  // queryable) ranges over the base table, so it also uses the base table's keys.
  private List<String> getPrimaryKeyColumns(Queryable queryable) {
    if (queryable.getIndexName() == null) {
      List<String> cols = new ArrayList<>();
      cols.add(collectionOptions.getPartitionKey());
      if (collectionOptions.getSortKey() != null) {
        cols.add(collectionOptions.getSortKey());
      }
      return cols;
    }
    for (IndexMeta index : getTableDescription().getIndexMeta()) {
      if (index.getIndexName().equals(queryable.getIndexName())) {
        return index.getPrimaryKeyList();
      }
    }
    throw new InvalidArgumentException(
        "Resolved index not found in table description: " + queryable.getIndexName());
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
    Queryable queryable = getBestQueryable(query);
    if (queryable.getIndexName() != null) {
      return "Index: " + queryable.getIndexName();
    }
    if (queryable.getKey() != null) {
      return "Table: " + collectionOptions.getTableName();
    }
    return "Scan: " + collectionOptions.getTableName();
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

  // orderingConsistent reports whether the ordering constraint is consistent with the sort key
  // field.
  // That is, either there is no OrderBy clause, or the clause specifies the sort field.
  private boolean orderingConsistent(Query query, String sortedField) {
    return StringUtils.isEmpty(query.getOrderByField())
        || query.getOrderByField().equals(sortedField);
  }

  protected boolean globalFieldIncluded(Query query, IndexMeta gi) {
    // The set of columns physically available from the index: its own primary-key columns (which
    // include the base table's primary key, folded in by Tablestore) plus its defined columns.
    Set<String> indexFields = new HashSet<>(gi.getPrimaryKeyList());
    indexFields.addAll(gi.getDefinedColumnsList());
    return indexUsableForQuery(query, indexFields);
  }

  // An index can serve a query when its columns can evaluate every predicate (the server-side
  // filter runs on the index read) and return every projected field. An unprojected query names no
  // fields: the projection requirement is vacuous and any column the index lacks -- including
  // schema-less attributes -- is recovered by hydrating the row from the base table.
  private boolean indexUsableForQuery(Query query, Set<String> indexFields) {
    Set<String> required = new HashSet<>();
    if (query.getFilters() != null) {
      for (Filter filter : query.getFilters()) {
        required.add(filter.getFieldPath());
      }
    }
    // Null/empty field paths mean an unprojected query: nothing to add to the projection
    // requirement. setFieldPaths(null) is reachable via the public Lombok setter, so guard it the
    // way the rest of the planner does rather than NPE on addAll(null).
    if (!ObjectUtils.isEmpty(query.getFieldPaths())) {
      required.addAll(query.getFieldPaths());
    }
    return indexFields.containsAll(required);
  }

  protected DescribeTableResponse getTableDescription() {
    if (tableDescription == null) {
      DescribeTableRequest describeTableRequest =
          new DescribeTableRequest(collectionOptions.getTableName());
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
    if (result != null) {
      return result;
    }

    result = getMatchingGlobalIndexesWithSortKey(query);
    if (result != null) {
      return result;
    }

    result = getMatchingBaseTableForPartitionOnly(query);
    if (result != null) {
      return result;
    }

    result = getMatchingGlobalIndexesForPartitionOnly(query);
    if (result != null) {
      return result;
    }

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
    return new Queryable(
        null, new Key(collectionOptions.getPartitionKey(), collectionOptions.getSortKey()));
  }

  // Using local index is good if sort key matches with that of the local index
  private Queryable getMatchingLocalIndexes(Query query) {
    for (IndexMeta index : getTableDescription().getIndexMeta()) {
      if (index.getIndexType() != IndexType.IT_LOCAL_INDEX) {
        continue;
      }

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
    return key != null
        && indexUsableForQuery(query, fields)
        && isValidSortKey(query, key.getSortKey());
  }

  private Queryable getMatchingGlobalIndexesWithSortKey(Query query) {
    for (IndexMeta index : getTableDescription().getIndexMeta()) {
      if (index.getIndexType() != IndexType.IT_GLOBAL_INDEX) {
        continue;
      }

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
      if (index.getIndexType() != IndexType.IT_GLOBAL_INDEX) {
        continue;
      }

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

  // Close cleans up any resources used by the Collection.
  @Override
  public void close() {
    this.executorService.shutdown();
    tableStoreClient.shutdown();
  }
}
