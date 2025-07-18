package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.nio.file.Path;

/**
 * An object representing a failed blob upload attempt
 */
@Builder
@Getter
public class FailedBlobUpload {
    private final Path source;
    private final Throwable exception;
}
