package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Wrapper object for directory download result metadata
 */
@Builder
@Getter
public class DirectoryDownloadResponse {
    private final List<FailedBlobDownload> failedTransfers;
}
