package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/** Wrapper object for list filters */
@Getter
public class ListBlobsRequest {

  private final String prefix;
  private final String delimiter;
  private final boolean includeCommonPrefixes;

  private ListBlobsRequest(Builder builder) {
    this.prefix = builder.prefix;
    this.delimiter = builder.delimiter;
    this.includeCommonPrefixes = builder.includeCommonPrefixes;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String prefix;
    private String delimiter;
    private boolean includeCommonPrefixes;

    public Builder withPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder withDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    /**
     * Includes entries derived from the delimiter as common prefixes when listing synchronously.
     *
     * @param includeCommonPrefixes whether to include common-prefix entries
     * @return this builder
     */
    public Builder withIncludeCommonPrefixes(boolean includeCommonPrefixes) {
      this.includeCommonPrefixes = includeCommonPrefixes;
      return this;
    }

    public ListBlobsRequest build() {
      return new ListBlobsRequest(this);
    }
  }
}
