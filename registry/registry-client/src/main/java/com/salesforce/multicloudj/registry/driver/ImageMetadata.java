package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

import java.time.Instant;
import java.util.List;

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
     * The size of the image in bytes
     */
    private final long size;
    
    /**
     * The media type of the image (e.g., "application/vnd.docker.distribution.manifest.v2+json")
     */
    private final String mediaType;
    
    /**
     * The image tag
     */
    private final String tag;
    
    /**
     * When the image was created/pushed
     */
    private final Instant createdAt;
    
    /**
     * Architecture of the image (e.g., "amd64", "arm64")
     */
    private final String architecture;
    
    /**
     * Operating system of the image (e.g., "linux", "windows")
     */
    private final String os;
    
    /**
     * List of tags associated with this image
     */
    private final List<String> tags;
}
