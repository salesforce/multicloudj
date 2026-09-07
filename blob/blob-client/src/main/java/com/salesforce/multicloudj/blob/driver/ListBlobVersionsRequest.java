package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Request object for listing versions of a specific blob.
 */
@Getter
public class ListBlobVersionsRequest {

  private final String key;

  /**
   * When {@code true}, the listing includes delete-marker entries in addition to content versions.
   * When {@code false} (the default), only content versions are returned. Defaults to {@code false}
   * so existing callers observe no behavioral change.
   */
  private final boolean includeDeleteMarkers;

  private ListBlobVersionsRequest(Builder builder) {
    this.key = builder.key;
    this.includeDeleteMarkers = builder.includeDeleteMarkers;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private boolean includeDeleteMarkers = false;

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    /**
     * Controls whether delete-marker entries are included in the version listing.
     *
     * @param includeDeleteMarkers {@code true} to include delete markers, {@code false} (default)
     *     to return only content versions.
     */
    public Builder withIncludeDeleteMarkers(boolean includeDeleteMarkers) {
      this.includeDeleteMarkers = includeDeleteMarkers;
      return this;
    }

    public ListBlobVersionsRequest build() {
      return new ListBlobVersionsRequest(this);
    }
  }
}
