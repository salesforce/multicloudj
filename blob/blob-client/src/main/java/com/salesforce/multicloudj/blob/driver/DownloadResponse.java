package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Wrapper object for download result metadata
 */
@Builder
@Getter
public class DownloadResponse {
    private final String key;
    private final BlobMetadata metadata;
}
