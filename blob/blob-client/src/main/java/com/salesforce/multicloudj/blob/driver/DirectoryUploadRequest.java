package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

/**
 * Wrapper object for directory upload data
 */
@Builder
@Getter
public class DirectoryUploadRequest {
    private final String localSourceDirectory;
    private final String prefix;
    private final boolean includeSubFolders;
}
