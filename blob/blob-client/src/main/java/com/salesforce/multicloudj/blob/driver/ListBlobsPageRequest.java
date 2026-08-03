package com.salesforce.multicloudj.blob.driver;

import com.salesforce.multicloudj.common.observability.OperationContext;
import lombok.Getter;

/** Request object for paginated list operations */
@Getter
public class ListBlobsPageRequest {

  private final String prefix;
  private final String delimiter;
  private final String paginationToken;
  private final Integer maxResults;

  /**
   * (Optional parameter) Per-call observability context carrying the correlation ID. The
   * correlation ID is never auto-generated; when it is null or missing it defaults to an empty
   * string and tracing is treated as disabled.
   */
  private final OperationContext operationContext;

  private ListBlobsPageRequest(Builder builder) {
    this.prefix = builder.prefix;
    this.delimiter = builder.delimiter;
    this.paginationToken = builder.paginationToken;
    this.maxResults = builder.maxResults;
    this.operationContext = builder.operationContext;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String prefix;
    private String delimiter;
    private String paginationToken;
    private Integer maxResults;
    private OperationContext operationContext;

    public Builder withPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder withDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder withPaginationToken(String paginationToken) {
      this.paginationToken = paginationToken;
      return this;
    }

    public Builder withMaxResults(Integer maxResults) {
      this.maxResults = maxResults;
      return this;
    }

    /**
     * Sets the per-call observability context carrying the correlation ID. The correlation ID is
     * never auto-generated; if not set (or if the context's correlation ID is null/empty) it
     * defaults to an empty string and tracing is treated as disabled.
     *
     * @param operationContext the observability context
     * @return this builder
     */
    public Builder withOperationContext(OperationContext operationContext) {
      this.operationContext = operationContext;
      return this;
    }

    public ListBlobsPageRequest build() {
      return new ListBlobsPageRequest(this);
    }
  }
}
