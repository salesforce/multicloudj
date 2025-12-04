package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * POJO for copyFrom requests.
 */
@Builder
@Getter
public class CopyFromRequest {

    /**
     * The name of the source bucket
     */
    private final String srcBucket;

    /**
     * The key of the blob to copy from the source bucket
     */
    private final String srcKey;

    /**
     * This field is optional. It's only used if you're copying from a bucket that has versioning enabled.
     * Explicitly specifying a versionId is only useful if you want to target the non-latest version of a blob.
     * This value should be null for non-versioned buckets, or if you want the latest version of a versioned blob.
     */
    private final String srcVersionId;

    /**
     * The key of the blob you're copying into in the destination (current) bucket
     */
    private final String destKey;
}
