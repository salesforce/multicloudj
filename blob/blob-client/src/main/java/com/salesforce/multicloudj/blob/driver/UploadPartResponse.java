package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * The response object returned after uploading an individual part of a multipartUpload
 */
@Getter
public class UploadPartResponse {

    private final int partNumber;
    private final String etag;
    private final long sizeInBytes;

    public UploadPartResponse(final int partNumber, final String etag, final long sizeInBytes){
        this.partNumber = partNumber;
        this.etag = etag;
        this.sizeInBytes = sizeInBytes;
    }

}
