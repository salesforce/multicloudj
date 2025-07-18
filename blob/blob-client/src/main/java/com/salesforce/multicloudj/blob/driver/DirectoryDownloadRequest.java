package com.salesforce.multicloudj.blob.driver;

import lombok.Builder;
import lombok.Getter;

import java.util.List;

/**
 * Wrapper object for directory download data
 */
@Builder
@Getter
public class DirectoryDownloadRequest {
    private final String prefixToDownload;
    private final String localDestinationDirectory;
    private final List<String> prefixesToExclude;
}
