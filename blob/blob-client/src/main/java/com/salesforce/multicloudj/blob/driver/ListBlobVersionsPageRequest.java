package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/** Request object for paginated listing of versions of a specific blob. */
@Getter
public class ListBlobVersionsPageRequest {

  private final String key;
  private final String paginationToken;
  private final Integer maxResults;

  private ListBlobVersionsPageRequest(Builder builder) {
    this.key = builder.key;
    this.paginationToken = builder.paginationToken;
    this.maxResults = builder.maxResults;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private String paginationToken;
    private Integer maxResults;

    public Builder withKey(String key) {
      this.key = key;
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

    public ListBlobVersionsPageRequest build() {
      return new ListBlobVersionsPageRequest(this);
    }
  }
}
