package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * The response object returned from completing a multipartUpload
 */
@Getter
public class MultipartUploadResponse {

    private final String etag;

    public MultipartUploadResponse(final String etag){
        this.etag = etag;
    }

}
