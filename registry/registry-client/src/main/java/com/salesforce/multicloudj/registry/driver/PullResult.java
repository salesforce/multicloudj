package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Result object for pulling a Docker image from a container registry.
 */
@Builder
@Getter
public class PullResult {
    /**
     * Metadata about the pulled image (digest, size, etc.)
     */
    private final ImageMetadata metadata;
    
    /**
     * The path where the image was saved (if saved to file system)
     */
    private final String savedPath;
}
