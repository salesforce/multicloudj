package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Wrapper object for directory upload result data
 */
@Builder
@Getter
public class DirectoryUploadResponse {
    private final List<FailedBlobUpload> failedTransfers;
}
