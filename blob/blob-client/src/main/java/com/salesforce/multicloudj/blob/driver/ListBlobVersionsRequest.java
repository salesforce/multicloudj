package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * Request object for listing versions of a specific blob.
 */
@Getter
public class ListBlobVersionsRequest {

  private final String key;

  /**
   * When {@code true}, the listing includes archived (delete-marker) entries in addition to content
   * versions. When {@code false} (the default), only content versions are returned. Defaults to
   * {@code false} so existing callers observe no behavioral change.
   */
  private final boolean includeArchived;

  private ListBlobVersionsRequest(Builder builder) {
    this.key = builder.key;
    this.includeArchived = builder.includeArchived;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String key;
    private boolean includeArchived = false;

    public Builder withKey(String key) {
      this.key = key;
      return this;
    }

    /**
     * Controls whether archived (delete-marker) entries are included in the version listing.
     *
     * @param includeArchived {@code true} to include archived entries, {@code false} (default) to
     *     return only content versions.
     */
    public Builder withIncludeArchived(boolean includeArchived) {
      this.includeArchived = includeArchived;
      return this;
    }

    public ListBlobVersionsRequest build() {
      return new ListBlobVersionsRequest(this);
    }
  }
}
