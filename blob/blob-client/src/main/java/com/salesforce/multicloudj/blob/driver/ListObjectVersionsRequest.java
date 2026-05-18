package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/** Request for listing all versions of an object key. */
@Getter
public class ListObjectVersionsRequest {

  private final String key;

  private ListObjectVersionsRequest(Builder builder) {
    this.key = builder.key;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    public ListObjectVersionsRequest build() {
      return new ListObjectVersionsRequest(this);
    }
  }
}
