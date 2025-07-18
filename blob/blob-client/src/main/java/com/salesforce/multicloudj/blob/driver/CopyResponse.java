package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;

/**
 * Wrapper object for copy result data
 */
@Builder
@Getter
public class CopyResponse {
    private final String key;

    /**
     * The versionId of this blob. This value only serves a purpose for buckets with versioning enabled,
     * although non-versioned buckets may still return a value for it. Non-versioned buckets should simply
     * ignore the versionId value as it serves no purpose for them.
     */
    private final String versionId;
    private final String eTag;
    private final Instant lastModified;
}
