package com.salesforce.multicloudj.blob.driver;

import java.util.List;
import lombok.Builder;
import lombok.Getter;

/** Wrapper object for directory download result metadata */
@Builder
@Getter
public class DirectoryDownloadResponse {
  private final List<FailedBlobDownload> failedTransfers;
  private final Long totalBytesRequested;
}
