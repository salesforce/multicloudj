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

  /**
   * Returns true when the full object key starts with any of {@link #prefixesToExclude}. A
   * null or empty exclusion list excludes nothing.
   *
   * @param key the full object key
   * @return true if the object must be skipped by the directory download
   */
  public boolean isExcluded(String key) {
    if (prefixesToExclude == null) {
      return false;
    }
    for (String prefixToExclude : prefixesToExclude) {
      if (key.startsWith(prefixToExclude)) {
        return true;
      }
    }
    return false;
  }
}
