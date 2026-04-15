package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Wrapper object for directory download data */
@Builder
@Getter
public class DirectoryDownloadRequest {
  private final String prefixToDownload;
  private final String localDestinationDirectory;
  private final List<String> prefixesToExclude;
  private final boolean transferStatusLoggingEnabled;
}
