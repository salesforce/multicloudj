package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

import java.util.List;

/**
 * Response object for paginated list operations that includes the blob list, truncation status, and pagination token
 */
@Getter
public class ListBlobsPageResponse {

    private final List<BlobInfo> blobs;
    private final boolean isTruncated;
    private final String nextPageToken;

    public ListBlobsPageResponse(List<BlobInfo> blobs, boolean isTruncated, String nextPageToken) {
        this.blobs = blobs;
        this.isTruncated = isTruncated;
        this.nextPageToken = nextPageToken;
    }

}