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
    private final String versionId;
    private final String eTag;
    private final Instant lastModified;
}
