package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Request object for listing versions of a specific blob.
 */
@Getter
public class ListBlobVersionsRequest {

  private final String key;

  private ListBlobVersionsRequest(Builder builder) {
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

    public ListBlobVersionsRequest build() {
      return new ListBlobVersionsRequest(this);
    }
  }
}
