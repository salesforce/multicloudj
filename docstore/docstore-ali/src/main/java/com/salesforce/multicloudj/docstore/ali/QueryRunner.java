package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.model.Direction;
import com.alicloud.openservices.tablestore.model.GetRangeRequest;
import com.alicloud.openservices.tablestore.model.GetRangeResponse;
import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.alicloud.openservices.tablestore.model.RangeRowQueryCriteria;
import com.alicloud.openservices.tablestore.model.Row;
import com.alicloud.openservices.tablestore.model.filter.Filter;
import java.util.List;
import lombok.Getter;

/**
 * Executes a query against the Tablestore native GetRange API (over the base table or a secondary
 * index by name), producing a resumable {@link PrimaryKey} cursor. GetRange is the query engine for
 * the driver: unlike the Tablestore SQL interface, its {@code nextStartPrimaryKey} is a genuine
 * positional cursor that supports caller-resumable pagination.
 *
 * <p>The runner is configured once by the planner with the target table/index name, the primary-key
 * range bounds, scan direction, an optional non-key column {@link Filter}, and the columns to
 * fetch. Each {@link #run} call fetches one page starting at a given cursor.
 *
 * <p><b>limit semantics:</b> when a column filter is set, Tablestore's {@code limit} caps rows
 * SCANNED, not rows RETURNED, so a single GetRange request can return fewer than {@code limit}
 * matches (or zero) while still handing back a continuation cursor. The iterator that drives this
 * runner is therefore responsible for looping (following the cursor) until it has accumulated the
 * caller's requested number of results or the cursor is exhausted.
 */
@Getter
public class QueryRunner {

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

  // Columns to return; null/empty means all columns of the matched rows.
  private final List<String> columnsToGet;

  public QueryRunner(
      SyncClient tableStoreClient,
      String tableName,
      PrimaryKey inclusiveStartPrimaryKey,
      PrimaryKey exclusiveEndPrimaryKey,
      Direction direction,
      Filter columnFilter,
      List<String> columnsToGet) {
    this.tableStoreClient = tableStoreClient;
    this.tableName = tableName;
    this.inclusiveStartPrimaryKey = inclusiveStartPrimaryKey;
    this.exclusiveEndPrimaryKey = exclusiveEndPrimaryKey;
    this.direction = direction;
    this.columnFilter = columnFilter;
    this.columnsToGet = columnsToGet;
  }

  public String queryPlan() {
    return "GetRange:" + tableName;
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
    if (columnsToGet != null && !columnsToGet.isEmpty()) {
      criteria.addColumnsToGet(columnsToGet);
    }

    GetRangeResponse response = tableStoreClient.getRange(new GetRangeRequest(criteria));
    items.addAll(response.getRows());
    return response.getNextStartPrimaryKey();
  }
}
