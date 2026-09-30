package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Getter;

/**
 * Response object for paginated list operations that includes the blob list, truncation status, and
 * pagination token
 */
@Getter
public class ListBlobsPageResponse {

  private final List<BlobInfo> blobs;
  private final List<String> commonPrefixes;
  private final boolean isTruncated;
  private final String nextPageToken;

  public ListBlobsPageResponse(List<BlobInfo> blobs, boolean isTruncated, String nextPageToken) {
    this(blobs, List.of(), isTruncated, nextPageToken);
  }

  public ListBlobsPageResponse(
      List<BlobInfo> blobs,
      List<String> commonPrefixes,
      boolean isTruncated,
      String nextPageToken) {
    this.blobs = blobs;
    this.commonPrefixes = commonPrefixes;
    this.isTruncated = isTruncated;
    this.nextPageToken = nextPageToken;
  }
}
