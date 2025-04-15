package com.salesforce.multicloudj.blob.driver;

import lombok.Getter;

/**
 * This object stores the identifying information for a multipart upload request.
 * It's used by the client whenever a multipart operation is being performed.
 */
@Getter
public class MultipartUpload {

    private final String bucket;
    private final String key;
    private final String id;

    public MultipartUpload(final String bucket, final String key, final String id){
        this.bucket = bucket;
        this.key = key;
        this.id = id;
    }

}
