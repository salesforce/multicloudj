package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Metadata about a Docker image in the registry.
 */
@Builder
@Getter
public class ImageMetadata {
    /**
     * The image digest (e.g., "sha256:abc123...")
     */
    private final String digest;
    
    /**
     * The image tag (e.g., "latest", "v1.0")
     * Null if the image was referenced by digest instead of tag.
     */
    private final String tag;
}
