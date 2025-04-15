package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Blob metadata data object
 */
@Builder
@Getter
public class BlobMetadata {

    private final String key;
    private final String versionId;
    private final String eTag;
    private final long objectSize;
    @Singular("metadata")
    private final Map<String, String> metadata;
    private final Instant lastModified;
}
