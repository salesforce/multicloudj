package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.time.Instant;
import java.util.Map;

/**
 * Blob metadata data object
 */
@Builder
@Getter
public class BlobMetadata {

    private final String key;

    /**
     * The versionId of this blob. This value only serves a purpose for buckets with versioning enabled,
     * although non-versioned buckets may still return a value for it. Non-versioned buckets should simply
     * ignore the versionId value as it serves no purpose for them.
     */
    private final String versionId;
    private final String eTag;
    private final long objectSize;
    @Singular("metadata")
    private final Map<String, String> metadata;
    private final Instant lastModified;
    private final byte[] md5;
    /**
     * Object lock information for this blob.
     * null if object lock is not configured.
     */
    private final ObjectLockInfo objectLockInfo;
}
