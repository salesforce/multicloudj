package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Value;

/**
 * Wrapper object for upload result data
 */
@Builder
@Value
public class UploadResponse {
    String key;

    /**
     * The versionId of this blob. This value only serves a purpose for buckets with versioning enabled,
     * although non-versioned buckets may still return a value for it. Non-versioned buckets should simply
     * ignore the versionId value as it serves no purpose for them.
     */
    String versionId;
    String eTag;
}
