package com.salesforce.multicloudj.registry.driver;

import lombok.Builder;
import lombok.Getter;

import java.io.InputStream;

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
     * The path where the image was saved (if saved to file system).
     * Null if image was written to OutputStream or returned as InputStream.
     */
    private final String savedPath;
    
    /**
     * InputStream for reading the image tar file (if using pullImage without destination).
     * Null if image was saved to file/Path/OutputStream.
     * The caller is responsible for closing this stream.
     */
    private final InputStream inputStream;
}
