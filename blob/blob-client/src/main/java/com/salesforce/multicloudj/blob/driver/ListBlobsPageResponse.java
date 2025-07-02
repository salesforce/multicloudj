package com.salesforce.multicloudj.blob.driver;

import java.util.List;

/**
 * Response object for paginated list operations that includes the blob list, truncation status, and pagination token
 */
public class ListBlobsPageResponse {

    private final List<BlobInfo> blobs;
    private final boolean isTruncated;
    private final String nextPageToken;

    public ListBlobsPageResponse(List<BlobInfo> blobs, boolean isTruncated, String nextPageToken) {
        this.blobs = blobs;
        this.isTruncated = isTruncated;
        this.nextPageToken = nextPageToken;
    }

    public List<BlobInfo> getBlobs() {
        return blobs;
    }

    public boolean isTruncated() {
        return isTruncated;
    }

    public String getNextPageToken() {
        return nextPageToken;
    }
} 