package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.BatchGetRowRequest;
import com.alicloud.openservices.tablestore.model.BatchGetRowResponse;
import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.MultiRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.PrimaryKeyBuilder;
import com.alicloud.openservices.tablestore.model.PrimaryKeyColumn;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import com.salesforce.multicloudj.common.exceptions.SubstrateSdkException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Executes a query against the Tablestore native GetRange API (over the base table or a secondary
 * index by name), producing a resumable {@link PrimaryKey} cursor. GetRange is the query engine for
 * the driver: unlike the Tablestore SQL interface, its {@code nextStartPrimaryKey} is a genuine
 * positional cursor that supports caller-resumable pagination.
 *
 * <p>The runner is configured once by the planner with the target table/index name, the primary-key
 * range bounds, scan direction, an optional non-key column {@link Filter}, the columns to fetch,
 * and the set of columns the caller may see. It may fetch more columns than the caller projected
 * (predicate columns are needed so the server-side filter, applied after columns_to_get, can
 * evaluate); each returned row is then trimmed back to the visible set. Each {@link #run} call
 * fetches one page starting at a given cursor.
 *
 * <p><b>base-table hydration:</b> when the runner ranges over a secondary index for an unprojected
 * query, the index physically carries only its own key and defined columns, so it cannot return a
 * row's schema-less attributes. In that case the runner is given a base table name and the base
 * primary-key column list; after each page it derives every row's base primary key (the full base
 * key is folded into every index key by Tablestore) and re-reads the full base rows in batches,
 * returning the base row's columns under the index row's primary key so the pagination cursor stays
 * on the index. Hydration and projection trimming are mutually exclusive: an unprojected query
 * hydrates and never trims, a projected query trims and never hydrates.
 *
 * <p><b>index consistency:</b> a Tablestore global secondary index is updated asynchronously and
 * is eventually consistent with the base table, and the index read and the base-table hydration
 * are two separate reads, not one snapshot. For a row updated concurrently within the index
 * replication-lag window, the hydrated columns reflect the CURRENT base values, which may differ
 * from the older index values the query's predicate and ordering were evaluated against; a
 * returned row may therefore momentarily not satisfy the predicate, or appear out of order if its
 * order-by attribute changed. This is a known, accepted characteristic of index-served queries,
 * documented here rather than corrected.
 *
 * <p><b>limit semantics:</b> when a column filter is set, Tablestore's {@code limit} caps rows
 * SCANNED, not rows RETURNED, so a single GetRange request can return fewer than {@code limit}
 * matches (or zero) while still handing back a continuation cursor. The iterator that drives this
 * runner is therefore responsible for looping (following the cursor) until it has accumulated the
 * caller's requested number of results or the cursor is exhausted.
 */
@Getter
public class QueryRunner {

  // Bounded re-drive budget for row-level BatchGetRow failures during hydration. This is on top of
  // the Tablestore client's own request-level retries: it re-issues only the sub-rows that came
  // back failed (throttled or partial server errors) so a transient hiccup does not surface as a
  // hard error, while still guaranteeing the read eventually fails loud rather than silently short.
  private static final int MAX_HYDRATION_RETRIES = 3;

  private final SyncClient tableStoreClient;

  // Name of the base table or the secondary index to range over.
  private final String tableName;

  // Full primary-key range bounds for the target table/index. inclusiveStart is replaced by the
  // resume cursor on subsequent pages; exclusiveEnd is constant.
  private final PrimaryKey inclusiveStartPrimaryKey;
  private final PrimaryKey exclusiveEndPrimaryKey;

  private final Direction direction;

  // Optional non-key predicate; null when the query has no attribute filters.
  private final Filter columnFilter;

  // Columns to FETCH from Tablestore; null/empty means all columns of the matched rows. May include
  // predicate columns beyond the caller's projection so the server-side column filter can evaluate
  // (columns_to_get is applied before the filter); those extra columns are trimmed back out via
  // visibleColumns before the rows reach the caller.
  private final List<String> columnsToGet;

  // Attribute columns the caller is allowed to SEE: the query's projection plus the target's
  // primary-key columns. Null means "all columns visible" -- no trimming. When non-null, each
  // returned row is trimmed to these columns, dropping predicate columns fetched only so the
  // server-side filter could evaluate.
  private final Set<String> visibleColumns;

  // Base table to hydrate each returned index row from; null means no hydration (a base-table/scan
  // query, or a projected index query). When set, the runner re-reads the full base row for every
  // index row on the page so schema-less attributes an index cannot carry are recovered.
  private final String baseTableName;

  // Ordered base-table primary-key column names, used to derive each row's base primary key from
  // its index primary key. Tablestore folds the full base primary key into every secondary index
  // key, so these columns are always present on an index row's primary key.
  private final List<String> baseKeyColumns;

  // Page-level batch size for the hydration BatchGetRow reads.
  private final int hydrationBatchSize;

  public QueryRunner(
      SyncClient tableStoreClient,
      String tableName,
      PrimaryKey inclusiveStartPrimaryKey,
      PrimaryKey exclusiveEndPrimaryKey,
      Direction direction,
      Filter columnFilter,
      List<String> columnsToGet,
      Set<String> visibleColumns,
      String baseTableName,
      List<String> baseKeyColumns,
      int hydrationBatchSize) {
    this.tableStoreClient = tableStoreClient;
    this.tableName = tableName;
    this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    this.direction = direction;
    this.columnFilter = columnFilter;
    this.columnsToGet = columnsToGet;
    this.visibleColumns = visibleColumns;
    this.baseTableName = baseTableName;
    this.baseKeyColumns = baseKeyColumns;
    this.hydrationBatchSize = hydrationBatchSize;
  }

  /**
   * Fetches a single GetRange page.
   *
   * @param startKey the inclusive start cursor for this page; when null the configured
   *     {@link #inclusiveStartPrimaryKey} (the range's left boundary) is used.
   * @param items rows for this page are appended here.
   * @return the {@code nextStartPrimaryKey} cursor to resume from, or null when the range is
   *     exhausted (no more pages).
   */
  public PrimaryKey run(PrimaryKey startKey, List<Row> items) {
    RangeRowQueryCriteria criteria = new RangeRowQueryCriteria(tableName);
    criteria.setInclusiveStartPrimaryKey(startKey != null ? startKey : inclusiveStartPrimaryKey);
    criteria.setExclusiveEndPrimaryKey(exclusiveEndPrimaryKey);
    criteria.setDirection(direction);
    criteria.setMaxVersions(1);
    // No explicit per-request limit: Tablestore already caps a GetRange response at 5000 rows /
    // 4 MB and returns a nextStartPrimaryKey cursor, and the iterator loops that cursor to satisfy
    // the caller's limit. Setting a smaller cap here would only add round-trips.
    if (columnFilter != null) {
      criteria.setFilter(columnFilter);
    }
    if (!ObjectUtils.isEmpty(columnsToGet)) {
      criteria.addColumnsToGet(columnsToGet);
    }

    GetRangeResponse response;
    try {
      response = tableStoreClient.getRange(new GetRangeRequest(criteria));
    } catch (RuntimeException e) {
      // Query.get() wraps only the iterator-construction path in its mapException boundary; the
      // getRange RPC fires later, during iteration, outside that boundary, so a raw provider
      // exception would escape un-mapped. Surface it as a SubstrateSdkException (the base
      // multicloudj type) so no raw provider exception leaves QueryRunner -- consistent with the
      // hydration batchGetRow handling below.
      // TODO: route through AliDocStore.mapException for fine-grained exception classification.
      throw new SubstrateSdkException("Query GetRange request failed", e);
    }
    List<Row> pageRows = response.getRows();
    if (baseTableName != null) {
      pageRows = hydrateFromBase(pageRows);
    }
    if (visibleColumns == null) {
      items.addAll(pageRows);
    } else {
      for (Row row : pageRows) {
        items.add(trimColumns(row, visibleColumns));
      }
    }
    // The pagination cursor stays the INDEX key: hydration only replaces columns, never the key.
    return response.getNextStartPrimaryKey();
  }

  // Replaces each index row's columns with the full base row read by its base primary key, so an
  // unprojected index query returns every column of the row -- including schema-less attributes the
  // index cannot carry. Preserves input order, keeps each row's index primary key (the cursor),
  // batches the base reads, and drops any row whose base row was concurrently deleted. Issues no
  // RPC for an empty page.
  private List<Row> hydrateFromBase(List<Row> indexRows) {
    if (indexRows.isEmpty()) {
      return indexRows;
    }
    List<PrimaryKey> baseKeys = new ArrayList<>(indexRows.size());
    for (Row indexRow : indexRows) {
      baseKeys.add(extractBaseKey(indexRow));
    }
    Map<PrimaryKey, Row> baseByKey = new HashMap<>();
    for (int i = 0; i < baseKeys.size(); i += hydrationBatchSize) {
      int end = Math.min(i + hydrationBatchSize, baseKeys.size());
      batchGetInto(baseKeys.subList(i, end), baseByKey);
    }
    List<Row> hydrated = new ArrayList<>(indexRows.size());
    for (int i = 0; i < indexRows.size(); i++) {
      Row baseRow = baseByKey.get(baseKeys.get(i));
      if (baseRow == null) {
        // A key absent from the map here can ONLY be a genuine concurrent deletion: batchGetInto
        // re-drives and ultimately throws on any FAILED sub-row, so a failure never reaches this
        // point. Drop the stale index entry rather than emitting a phantom.
        continue;
      }
      hydrated.add(new Row(indexRows.get(i).getPrimaryKey(), baseRow.getColumns()));
    }
    return hydrated;
  }

  // Derives the base-table primary key of an index row: the base key columns read off the index
  // row's primary key, in base-table key order. Ordering matters because PrimaryKey equality is
  // position-sensitive and these keys are used as map lookup keys against the base rows.
  private PrimaryKey extractBaseKey(Row indexRow) {
    PrimaryKeyBuilder builder = PrimaryKeyBuilder.createPrimaryKeyBuilder();
    for (String column : baseKeyColumns) {
      PrimaryKeyColumn keyColumn = indexRow.getPrimaryKey().getPrimaryKeyColumn(column);
      if (keyColumn == null) {
        // Tablestore folds the full base primary key into every secondary index key, so a base-key
        // column is always expected on an index row's primary key. Its absence violates that
        // invariant; fail loud naming the missing column and the target being read rather than
        // NPE into an opaque UnknownException.
        throw new SubstrateSdkException(
            String.format(
                "Base-key column '%s' is missing from the index row's primary key while reading"
                    + " '%s'; cannot derive the base-table key for hydration",
                column, tableName));
      }
      builder.addPrimaryKeyColumn(column, keyColumn.getValue());
    }
    return builder.build();
  }

  // Reads one batch of base rows by primary key and indexes the successful results by primary key.
  // No columns_to_get is set, so the full row (all columns, including schema-less attributes) is
  // returned. Row-level failures (a sub-row that came back with isSucceed()==false after the
  // client's own request-level retry budget -- e.g. throttling or a partial server error) are NOT
  // deletions and must never be silently dropped: dropping one would return fewer rows than exist
  // while the pagination cursor advances past the gap, permanently skipping that row. Such failures
  // are re-driven up to MAX_HYDRATION_RETRIES times via BatchGetRowRequest.createRequestForRetry
  // (which re-issues only the still-failing sub-rows, preserving the criteria); if any sub-row is
  // still failing after that budget, the whole read fails loud. A SUCCEEDED result with a null row
  // is a genuine concurrent deletion and is intentionally omitted from the map (dropped upstream).
  private void batchGetInto(List<PrimaryKey> chunk, Map<PrimaryKey, Row> out) {
    MultiRowQueryCriteria criteria = new MultiRowQueryCriteria(baseTableName);
    criteria.setMaxVersions(1);
    for (PrimaryKey pk : chunk) {
      criteria.addRow(pk);
    }
    BatchGetRowRequest request = new BatchGetRowRequest();
    request.addMultiRowQueryCriteria(criteria);

    BatchGetRowResponse response = tableStoreClient.batchGetRow(request);
    accumulateSucceeded(response, out);

    List<BatchGetRowResponse.RowResult> failed = response.getFailedRows();
    int attempt = 0;
    while (!failed.isEmpty() && attempt < MAX_HYDRATION_RETRIES) {
      request = request.createRequestForRetry(failed);
      response = tableStoreClient.batchGetRow(request);
      accumulateSucceeded(response, out);
      failed = response.getFailedRows();
      attempt++;
    }
    if (!failed.isEmpty()) {
      throw new SubstrateSdkException(
          String.format(
              "Base-table hydration failed: %d sub-row(s) could not be read after %d retries;"
                  + " first error: %s",
              failed.size(), MAX_HYDRATION_RETRIES, failed.get(0).getError()));
    }
  }

  // Indexes every SUCCEEDED sub-row that carries a row by its primary key. A succeeded result with
  // a null row is a genuine concurrent deletion (or not-found) and is intentionally omitted so the
  // stale index entry is dropped upstream; failed sub-rows are excluded here and handled by the
  // retry logic in batchGetInto.
  private static void accumulateSucceeded(BatchGetRowResponse response, Map<PrimaryKey, Row> out) {
    for (BatchGetRowResponse.RowResult result : response.getSucceedRows()) {
      if (result.getRow() != null) {
        out.put(result.getRow().getPrimaryKey(), result.getRow());
      }
    }
  }

  /**
   * Returns a copy of {@code row} keeping only the attribute columns whose name is in
   * {@code visible}. The primary key is carried over via {@link Row#getPrimaryKey()}, so both the
   * decode path and the pagination cursor still see the full key. Used to drop predicate columns
   * that were fetched only so the server-side column filter could evaluate but that the caller's
   * projection did not request.
   */
  static Row trimColumns(Row row, Set<String> visible) {
    List<Column> kept = new ArrayList<>();
    for (Column column : row.getColumns()) {
      if (visible.contains(column.getName())) {
        kept.add(column);
      }
    }
    return new Row(row.getPrimaryKey(), kept);
  }
}
