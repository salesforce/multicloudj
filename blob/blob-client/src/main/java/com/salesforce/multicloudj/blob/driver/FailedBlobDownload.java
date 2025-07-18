package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.nio.file.Path;

/**
 * An object representing a failed blob download attempt
 */
@Builder
@Getter
public class FailedBlobDownload {
    private final Path destination;
    private final Throwable exception;
}
