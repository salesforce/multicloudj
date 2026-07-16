package com.salesforce.multicloudj.docstore.ali;

import com.alicloud.openservices.tablestore.model.PrimaryKey;
import com.salesforce.multicloudj.docstore.driver.PaginationToken;
import lombok.Getter;
import lombok.Setter;

/**
 * Alibaba Tablestore pagination token.
 *
 * <p>Wraps the {@link PrimaryKey} continuation cursor returned by Tablestore's GetRange operation
 * ({@code GetRangeResponse.getNextStartPrimaryKey()}). This is a positional bookmark: it identifies
 * where, in the table's (or index's) primary-key order, the next page resumes. Feeding it back
 * as the {@code inclusiveStartPrimaryKey} of a subsequent GetRange request continues the scan from
 * exactly that point, so a caller can obtain a token from one query and resume in a separate,
 * later query.
 *
 * <p>A {@code null}/empty key means the previous page reached the end of the range, which the
 * docstore contract treats as "no more results".
 *
 * <p>Note: this is deliberately NOT the SQL {@code nextSearchToken}. That value is an internal,
 * size-driven streaming continuation that Tablestore only emits when a single result set overflows
 * a server response page; it is not a caller-resumable positional bookmark and comes back null for
 * bounded result sets. GetRange's {@code nextStartPrimaryKey} is the correct cursor for cross-call
 * pagination.
 */
public class AliPaginationToken implements PaginationToken {

  @Getter @Setter private PrimaryKey nextStartPrimaryKey;

  public AliPaginationToken(PrimaryKey nextStartPrimaryKey) {
    this.nextStartPrimaryKey = nextStartPrimaryKey;
  }

  @Override
  public boolean isEmpty() {
    return nextStartPrimaryKey == null || nextStartPrimaryKey.isEmpty();
  }
}
