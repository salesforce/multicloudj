package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Getter;

/** Response object for paginated listing of versions of a specific blob. */
@Getter
public class ListBlobVersionsPageResponse {

  private final List<BlobInfo> versions;
  private final boolean isTruncated;
  private final String nextPageToken;

  public ListBlobVersionsPageResponse(
      List<BlobInfo> versions, boolean isTruncated, String nextPageToken) {
    this.versions = versions;
    this.isTruncated = isTruncated;
    this.nextPageToken = nextPageToken;
  }
}
