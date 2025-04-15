package com.salesforce.multicloudj.blob.driver;

import java.util.List;

public class ListBlobsBatch {

    private final List<BlobInfo> blobs;
    private final List<String> commonPrefixes;

    public ListBlobsBatch(List<BlobInfo> blobs, List<String> commonPrefixes) {
        this.blobs = blobs;
        this.commonPrefixes = commonPrefixes;
    }

    public List<BlobInfo> getBlobs() {
        return blobs;
    }

    public List<String> getCommonPrefixes() {
        return commonPrefixes;
    }
}
